#include "paxos_impl.h"
#include "../common/utils.h"
#include "../common/collect.h"

void PaxosImpl::handle_proposals() {
  while (true) {
    CommandT c;
    while (!command_chan.try_pop(c)) {}

    while (!has_valid_leader()) {}
    if (view.self_role != LEADER) {
      // if not leader, forward to leader
      invokers[view.leader_id]->propose(*c.second);
      c.first->finish_propose(true);
      continue;
    }

    mtx.lock();
    while (!has_valid_leader()) {
      std::cout << "not valid detected" << std::endl;
      mtx.unlock();
      while (!has_valid_leader())
        boost::this_fiber::yield();
      mtx.lock();
    }
    // if I'm leader
    auto inst_id = next_id++;
    auto epoch = view.epoch;
    auto self_id = view.self_id;
    c.first->epoch = view.epoch;
    c.first->id = inst_id;
    auto strs = c.second;
    std::atomic<uint32_t> counter;
    std::atomic<uint32_t> finished;
    counter.store(1, std::memory_order_release);
    finished.store(1, std::memory_order_release);
    std::vector<Collect<std::vector<PaxosMsg>>> collects(view.vm.size() - 1);
    uint64_t indexer = 0;
    // TODO: batching
    for (auto &invoker: invokers) {
      if (invoker.first != view.self_id) {
        auto index = indexer++;
        boost::fibers::fiber([=, &counter, &finished, &collects]() {
          auto commit_result =
            invoker.second->
              commit(view.epoch,
                     view.self_id,
                     {inst_id},
                     *strs);

          if (commit_result.first) {
            if (commit_result.second->at(0).result() == PaxosMsg::SUCCESS) {
              collects[index].collect(std::move(commit_result.second));
              counter.fetch_add(1, std::memory_order_release);
            } else if (commit_result.second->at(0).result() == PaxosMsg::FOLLOWUP) {
              if (commit_result.second->at(0).epoch() > view.epoch) {
                view.leader_id = commit_result.second->at(0).node_id();
                view.self_role = PaxosRole::FOLLOWER;
              }
            }
          }
          finished.fetch_add(1, std::memory_order_release);
        }).detach();
      }
    }
    PaxosRecord record;
    auto result = records.insert({inst_id, record});
    if (result.second) {
      result.first->second.promised_epoch = view.epoch;
      result.first->second.status = PREPARED;
      result.first->second.value = (*c.second)[0];
      auto quorum_size = (view.vm.size() + 1) / 2;

      while (true) {
        if (finished.load(std::memory_order_acquire) == view.vm.size()) {
          break;
        }

        boost::this_fiber::yield();
      }

      if (counter.load(std::memory_order_acquire) < quorum_size) {
        for (auto &collect : collects) {
          if (collect.has_value()) {
            if (collect->at(0).result() == PaxosMsg::FOLLOWUP) {
              if (collect->at(0).epoch() > view.epoch) {
                view.epoch = collect->at(0).epoch();
                view.leader_id = collect->at(0).node_id();
                view.self_role = FOLLOWER;
              }
            }
          }
        }
        c.first->finish_propose(false);
        mtx.unlock();
        continue;
      }

      for (auto& invoker: invokers) {
        if (invoker.first != view.self_id) {
          boost::fibers::fiber([=, &invoker]() {
            invoker.second->
              learn(epoch,
                    self_id,
                    {inst_id},
                    *strs);
            }).detach();
        }
      }
      // TODO: persistent learned result
      records[inst_id].status = LEARNED;
      boost::this_fiber::yield();
      c.first->finish_propose(true);
      mtx.unlock();
      continue;
    } else {
      // if not leader, forward to leader
      //mtx.unlock();
      //invokers[view.leader_id]->propose(*c.second);
      //c.first->finish_propose(true);
      abort();
    }
  }
}

std::vector<InstanceIDT>
PaxosImpl::get_learned_ranges() {
  std::vector<uint64_t> ranges;
  if (compacted_records.empty()) {
    if (records.empty())
      return ranges;
    ranges.push_back(1);
    ranges.push_back(compacted_records.size());
  }

  uint64_t last;

  if (records.empty())
    return ranges;

  auto iter = records.cbegin();
  last = iter->first;
  ranges.push_back(last);
  iter++;
  while (iter != records.cend()) {
    if (iter->second.status == LEARNED && iter->first - last > 1) {
      ranges.push_back(last);
      ranges.push_back(iter->first);
      last = iter->first;
    } else
      last++;
    iter++;
  }
  ranges.push_back(last);

  return ranges;
}

std::vector<InstanceIDT>
PaxosImpl::get_known_ranges() {
  std::vector<uint64_t> ranges;
  if (compacted_records.empty()) {
    if (records.empty()) {
      return ranges;
    }
    ranges.push_back(1);
    ranges.push_back(compacted_records.size());
  }

  uint64_t last;

  if (records.empty())
    return ranges;

  auto iter = records.cbegin();
  last = iter->first;
  ranges.push_back(last);
  iter++;
  while (iter != records.cend()) {
    if (iter->first - last > 1) {
      ranges.push_back(last);
      ranges.push_back(iter->first);
      last = iter->first;
    } else
      last++;
    iter++;
  }
  ranges.push_back(last);

  return ranges;
}


void
PaxosImpl::init_invokers() {
  for (const auto&v : view.vm) {
    if (v.first != view.self_id) {
      auto invoker = new PaxosInvoker(
          grpc::CreateChannel(
            v.second,
            grpc::InsecureChannelCredentials()));
      invokers.insert({v.first, invoker});
    }
  }
}

bool
PaxosImpl::init(PaxosView&& view_, PaxosConfig&& config_) {
  leader_valid.store(true, std::memory_order_release);
  running_for_leader.store(false, std::memory_order_release);
  next_id = 1;
  view = view_;
  config = config_;
  std::random_device r;

  // Choose a random vote timeout
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(PAXOS_RANDOM_WAIT_LOW, PAXOS_RANDOM_WAIT_HIGH);
  vote_timeout = uniform_dist(e1);
  _waiting_handler = std::thread([=]() {
    ServerBuilder builder;
    builder.AddListeningPort(view.vm[view.self_id], grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    auto service = std::move(builder.BuildAndStart());
    service->Wait();
  });
  _receiving_handler = std::thread(std::bind(&PaxosImpl::handle_proposals, this));
  return true;
}

Status
PaxosImpl::propose(grpc::ServerContext* context,
                   grpc::ServerReaderWriter<PaxosMsg, PaxosMsg>* stream) {
  PaxosMsg request, reply;
  if (view.leader_id != view.self_id) {
    reply.set_result(PaxosMsg::FAILURE);
    reply.set_node_id(view.leader_id);
    reply.set_epoch(view.epoch);
    stream->Write(reply);
    return Status::OK;
  }
  if (!context->IsCancelled()) {
    std::vector<std::shared_ptr<ProposeToken>> tokens;
    while (stream->Read(&request)) {
      tokens.push_back(async_propose(request.value()));
    }
    for (auto &token : tokens) {
      token->wait();
      if (token->get_result()) {
        reply.set_result(PaxosMsg::SUCCESS);
      } else {
        reply.set_result(PaxosMsg::FAILURE);
      }
      stream->Write(reply);
    }
    return Status::OK;
  }
  return Status::CANCELLED;
}


Status
PaxosImpl::commit(grpc::ServerContext* context,
              grpc::ServerReaderWriter<PaxosMsg, PaxosMsg>* stream) {
  if (context->IsCancelled()) {
    return Status::CANCELLED;
  }
  PaxosMsg request, reply;
  auto lock_res = mtx.try_lock();
  while (!lock_res) {
    if (running_for_leader.load(std::memory_order_acquire)) {
      reply.set_result(PaxosMsg::FAILURE);
      reply.set_node_id(view.self_id);
      stream->Write(reply);
      return Status::OK;
    }
    std::this_thread::yield();
    lock_res = mtx.try_lock();
  }
  while (stream->Read(&request)) {
    auto request_epoch = request.epoch();
    auto request_instance_id = request.instance_id();
    auto &request_value = request.value();
    // should consider if the node never
    // sees the prepare from new leader
    if (request_epoch >= view.epoch) {
      if (request_epoch > view.epoch) {
        view.epoch = request_epoch;
        view.leader_id = request.node_id();
        view.self_role = FOLLOWER;
      }
      auto record = PaxosRecord(PREPARED, request_epoch, string());
      auto kv_pair = std::make_pair(request_instance_id, record);
      auto inst_res = records.insert(kv_pair);
      reply.set_result(PaxosMsg::SUCCESS);
      if (inst_res.second) {
          inst_res.first->second.value = request_value;
      } else {
        if (inst_res.first->second.status == LEARNED) {
          reply.set_result(PaxosMsg::CONFLICT);
        }
        inst_res.first->second.promised_epoch = kv_pair.second.promised_epoch;
        if (!bytes_eq(inst_res.first->second.value, request_value)) {
          inst_res.first->second.value = request_value;
        }
      }
      // TODO: persistent commit result
    } else {
      reply.set_result(PaxosMsg::FOLLOWUP);
      reply.set_epoch(view.epoch);
      reply.set_node_id(view.leader_id);
      std::cout << "ask to followup" << std::endl;
      goto commit_out;
    }
    stream->Write(reply);
  }
commit_out:
  mtx.unlock();
  return Status::OK;
}

Status
PaxosImpl::learn(grpc::ServerContext* context,
              grpc::ServerReaderWriter<PaxosMsg, PaxosMsg>* stream) {
  if (context->IsCancelled()) {
    return Status::CANCELLED;
  }
  PaxosMsg request, reply;
  auto lock_res = mtx.try_lock();
  while (!lock_res) {
    if (running_for_leader.load(std::memory_order_acquire)) {
      reply.set_result(PaxosMsg::FAILURE);
      reply.set_node_id(view.self_id);
      stream->Write(reply);
      return Status::OK;
    }
    std::this_thread::yield();
    lock_res = mtx.try_lock();
  }
  while (stream->Read(&request)) {
    auto request_epoch = request.epoch();
    auto request_instance_id = request.instance_id();
    auto &request_value = request.value();
    auto record = PaxosRecord(LEARNED, 0, string());
    auto kv_pair = std::make_pair(request_instance_id, record);
    auto inst_res = records.insert(kv_pair);
    reply.set_result(PaxosMsg::SUCCESS);
    if (inst_res.second) {
      if (request_value.length() != 0) {
        inst_res.first->second.value = request_value;
      }
    } else {
      if (inst_res.first->second.status == LEARNED) {
        if (!bytes_eq(inst_res.first->second.value, request_value))
          reply.set_result(PaxosMsg::CONFLICT);
        stream->Write(reply);
        continue;
      } else {
        inst_res.first->second.status = LEARNED;
        inst_res.first->second.promised_epoch = request_epoch;
        if (!bytes_eq(inst_res.first->second.value, request_value))
          inst_res.first->second.value = request_value;
      }
    }
    stream->Write(reply);
  }
learn_out:
  mtx.unlock();
  return Status::OK;
}

Status
PaxosImpl::get_vote(grpc::ServerContext* context,
             const PaxosMsg* request,
             PaxosMsg* response) {
  if (context->IsCancelled()) {
    return Status::CANCELLED;
  }
  if (request->epoch() > view.epoch) {
    EpochT viewed;
    mtx.lock();
    if (request->epoch() > view.epoch) {
      response->set_result(PaxosMsg::SUCCESS);

      if (request->epoch() < voted_epoch) {
        response->set_result(PaxosMsg::FAILURE);
        response->set_epoch(voted_epoch);
      }

      if (request->epoch() > voted_epoch) {
        voted_epoch = request->epoch();
        voted_for = request->node_id();
      } else if (request->epoch() == voted_epoch &&
          request->node_id() != voted_for) {
        response->set_epoch(voted_epoch);
        response->set_result(PaxosMsg::FAILURE);
      }
      mtx.unlock();
      return Status::OK;
    } else
      mtx.unlock();
  }
  response->set_epoch(view.epoch);
  response->set_result(PaxosMsg::FAILURE);
  return Status::OK;
}


Status
PaxosImpl::ask_follow(grpc::ServerContext* context,
                      const PaxosMsg* request,
                      grpc::ServerWriter<PaxosMsg>* writer) {
  if (context->IsCancelled())
    return Status::CANCELLED;
  PaxosMsg reply;

  if (request->epoch() >= view.epoch) {
    leader_valid.store(false, std::memory_order_release);
  }
  mtx.lock();
  if (request->epoch() >= view.epoch) {
    view.epoch = request->epoch();
    view.leader_id = request->node_id();
    view.self_role = FOLLOWER;
    auto& req_ranges =request->ranges();
    auto self_ranges = get_known_ranges();
    auto diffs = diff_ranges(req_ranges, self_ranges);

    // reply values
    if (diffs.second.size() != 0) {
      auto itera = diffs.second.cbegin();
      while (itera != diffs.second.cend()) {
        for (unsigned i = *itera;i <= *(itera + 1);++i) {
          reply.set_instance_id(i);
          reply.set_epoch(records[i].promised_epoch);
          reply.set_value(records[i].value);
          reply.set_val_learned(records[i].status == LEARNED);
          reply.set_result(PaxosMsg::SUCCESS);
          writer->Write(reply);
        }
        itera += 2;
      }
    } else {
      reply.set_instance_id(0);
      reply.set_result(PaxosMsg::SUCCESS);
      writer->Write(reply);
    }

    // reply querying ranges
    if (diffs.first.size() != 0) {
      for (auto &v: diffs.first) {
        reply.add_ranges(v);
      }
      reply.set_result(PaxosMsg::REQUIRE);
      writer->Write(reply);
    }
  } else {
    reply.set_epoch(view.epoch);
    reply.set_node_id(view.leader_id);
    reply.set_result(PaxosMsg::FAILURE);
    writer->Write(reply);
  }
  leader_valid.store(true, std::memory_order_release);
  mtx.unlock();

  return Status::OK;
}

std::shared_ptr<ProposeToken>
PaxosImpl::async_propose(const string& value) {
  std::vector<string> values = {value};
  auto c = std::make_shared<std::vector<string>>(std::move(values));
  auto token = std::shared_ptr<ProposeToken>(new ProposeToken());
  command_chan.push(std::make_pair(token, c));
  return token;
}

void
PaxosImpl::debug_request_leader() {
  mtx.lock();
  running_for_leader.store(true, std::memory_order_release);
  if (leader_valid.load(std::memory_order_acquire) &&
      view.self_id == view.leader_id) {
    mtx.unlock();
    running_for_leader.store(false, std::memory_order_release);
    return;
  }
  leader_valid.store(false, std::memory_order_release);
  view.self_role = CANDIDATE;
  view.epoch += 1;
  auto quorum_size = (view.vm.size() + 1) / 2;
  mtx.unlock();
  std::atomic<uint32_t> counter;
  std::atomic<uint32_t> finished;
  std::vector<boost::fibers::fiber> fbs;
  counter.store(1, std::memory_order_release);
  finished.store(1, std::memory_order_release);
  for (auto &invoker: invokers) {
    if (invoker.first != view.self_id) {
      boost::fibers::fiber([=, &counter, &finished]() {
        auto vote_result =
          invoker.second->get_vote(view.epoch, view.self_id);
        if (vote_result.first) {
          if (vote_result.second.result() == PaxosMsg::SUCCESS)
            counter.fetch_add(1, std::memory_order_release);
        }
        finished.fetch_add(1, std::memory_order_release);
      }).detach();
    }
  }
  while (true) {
    boost::this_fiber::yield();
    auto t = counter.load(std::memory_order_acquire);
    if (t >= quorum_size)
      break;

    if (finished.load(std::memory_order_acquire) == view.vm.size()) {
      break;
    }
  }
  if (counter.load(std::memory_order_acquire) < quorum_size) {
    std::cout << "request_leader failure" << std::endl;
    running_for_leader.store(false, std::memory_order_release);
    return;
  }

  // if success

  mtx.lock();
  view.self_role = LEADER;
  view.leader_id = view.self_id;
  leader_valid.store(true, std::memory_order_release);
  // Prepare
  uint64_t indexer = 0;
  finished.store(1, std::memory_order_release);
  auto learned_ranges = get_learned_ranges();
  std::vector<Collect<std::vector<PaxosMsg>>> collects(view.vm.size() - 1);
  std::vector<std::vector<uint64_t>> require_ranges(view.vm.size() - 1);
  // TODO: collect all result
  for (auto &invoker: invokers) {
    if (invoker.first != view.self_id) {
      auto index = indexer++;
      boost::fibers::fiber([=, &counter, &finished, &collects, &learned_ranges]() {
        auto vote_result =
          invoker.second->ask_follow(view.epoch, view.self_id, learned_ranges);

        if (vote_result.first) {
          collects[index].collect(std::move(vote_result.second));
          
        }
        finished.fetch_add(1, std::memory_order_release);
      }).detach();
    }
  }
  while (true) {
    boost::this_fiber::yield();

    if (finished.load(std::memory_order_acquire) == view.vm.size()) {
      break;
    }
  }

  indexer = 0;
  // record all values received from other nodes
  next_id = std::max(next_id,
              std::max(compacted_records.size() + 1,
                       records.size() == 0 ? next_id : records.rend()->first + 1));
  for (auto &collect: collects) {
    auto index = indexer++;
    if (collect.has_value()) {
      auto iter = collect->begin();
      if (iter->result() == PaxosMsg::FAILURE) {
        if (iter->epoch() > view.epoch) {
          view.leader_id = iter->node_id();
          view.epoch = iter->epoch();
          view.self_role = FOLLOWER;
          mtx.unlock();
          running_for_leader.store(false, std::memory_order_release);
          return;
        }
      } else {
        while (iter != collect->end() &&
               iter->result() == PaxosMsg::SUCCESS &&
               iter->instance_id() != 0) {
          PaxosRecord record;
          record.value = std::move(iter->value());
          record.promised_epoch = iter->epoch();
          record.status = iter->val_learned() ? LEARNED : PREPARED;
          if (iter->instance_id() + 1 > next_id)
            next_id = iter->instance_id();
          auto record_pair = std::make_pair(iter->instance_id(), std::move(record));
          auto result = records.insert(std::move(record_pair));
          if (!result.second) {
            if (records[result.first->first].status == LEARNED)
              continue;
            if (result.first->second.promised_epoch >
                records[result.first->first].promised_epoch) {
              records[result.first->first].value = result.first->second.value;
            }
          }
          iter++;
        }

        if (iter != collect->end() &&
            iter->result() == PaxosMsg::REQUIRE) {
          std::move(iter->ranges().begin(),
                    iter->ranges().end(),
                    std::back_inserter(require_ranges[index]));
        }
      }
    }
  }

  // find empty holes and set them blank
  PaxosRecord blank;
  blank.status = LEARNED;
  for (uint64_t i = compacted_records.size() + 1;i < next_id;++i) {
    if (records.find(i) == records.end()) {
      records[i] = blank;
    }
  }

  // TODO: persistent
  // finish commit
  finished.store(1, std::memory_order_release);
  std::vector<InstanceIDT> inst_ids;
  std::vector<std::string> values;
  for (auto& record: records) {
    if (record.second.status == PREPARED) {
      inst_ids.push_back(record.first);
      values.push_back(record.second.value);
    }
  }

  if (!inst_ids.empty()) {
    indexer = 0;
    std::vector<Collect<std::vector<PaxosMsg>>> collects2(view.vm.size() - 1);

    for (auto &invoker: invokers) {
      auto index = indexer++;
      if (invoker.first != view.self_id) {
        boost::fibers::fiber([=, &finished, &inst_ids, &values, &collects2]() {
          auto commit_result =
            invoker.second->
              commit(view.epoch,
                     view.self_id,
                     inst_ids,
                     values);

          if (commit_result.first) {
            collects2[index].collect(std::move(commit_result.second));
          }
          finished.fetch_add(1, std::memory_order_release);
        }).detach();
      }
    }

    while (finished.load(std::memory_order_acquire) != view.vm.size()) {
      boost::this_fiber::yield();
    }

    int count = 0;
    for (auto &collect:collects2) {
      auto &msg = collect->at(0);
      if (msg.result() == PaxosMsg::SUCCESS)
        count++;
      else if (msg.result() == PaxosMsg::FOLLOWUP) {
        if (msg.epoch() > view.epoch) {
          view.leader_id = msg.node_id();
          view.epoch = msg.epoch();
          view.self_role = FOLLOWER;
          mtx.unlock();
          running_for_leader.store(false, std::memory_order_release);
          return;
        }
      } else
        abort();
    }

    if (count < quorum_size) {
      // TODO update fail
      mtx.unlock();
      running_for_leader.store(false, std::memory_order_release);
      return;
    }
  }

  // finish learn
  indexer = 0;
  for (auto& invoker: invokers) {
    auto index = indexer++;
    if (invoker.first != view.self_id) {
      auto f = boost::fibers::fiber([=, &invoker](std::vector<InstanceIDT>&& require_ranges) mutable {
        auto iter = require_ranges.begin();
        while (iter != require_ranges.end()) {
          for (InstanceIDT i = *iter;i <= *(iter + 1);++i) {
            inst_ids.push_back(i);
            if (i > compacted_records.size()) {
              values.push_back(records[i].value);
            } else
              values.push_back(compacted_records[i - 1].get_value());
          }
          iter += 2;
        }
        if (!inst_ids.empty()) {
          boost::fibers::fiber([=, &invoker](std::vector<InstanceIDT>&& inst_ids,
                                             std::vector<std::string>&& values) {
            invoker.second->
              learn(view.epoch,
                    view.self_id,
                    inst_ids,
                    values);
          }, std::move(inst_ids), std::move(values)).detach();
        }
      }, std::move(require_ranges[index]));
      fbs.push_back(std::move(f));
    }
  }

  for (auto &f: fbs)
    f.join();

  mtx.unlock();
  running_for_leader.store(false, std::memory_order_release);
  return;
}
