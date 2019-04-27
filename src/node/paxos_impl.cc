#include "paxos_impl.h"
#include "../common/utils.h"

PaxosMsg
PaxosInvoker::propose(const std::string& value) {
  PaxosMsg request, reply;
  ClientContext context;
  auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(PAXOS_COMMIT_TIMEOUT * 2);
  request.set_value(value);
  auto status = stub_->propose(&context, request, &reply);

  if (status.ok()) {
    return reply;
  } else {
    std::cerr << status.error_code() << ": " << status.error_message() << std::endl;
    abort();
  }
}

PaxosMsg
PaxosInvoker::commit(EpochT epoch, NodeIDT node_id,
                           InstanceIDT instance_id,
                           const std::string* value) {
  PaxosMsg request, reply;
  if (value != nullptr)
    request.set_value(*value);
  else
    request.set_value(string());

  request.set_epoch(epoch);
  request.set_node_id(node_id);
  request.set_instance_id(instance_id);
  ClientContext context;
  auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(PAXOS_COMMIT_TIMEOUT);
  Status status = stub_->commit(&context, request, &reply);

  if (status.ok()) {
    return reply;
  } else {
    std::cerr << status.error_code() << ": " << status.error_message() << std::endl;
    abort();
  }
}

PaxosMsg
PaxosInvoker::learn(EpochT epoch, NodeIDT node_id,
                         InstanceIDT instance_id,
                         const std::string* value) {
  PaxosMsg request, reply;
  if (value != nullptr)
    request.set_value(*value);
  else
    request.set_value(string());

  request.set_epoch(epoch);
  request.set_node_id(node_id);
  request.set_instance_id(instance_id);
  ClientContext context;
  auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(PAXOS_LEARN_TIMEOUT);
  Status status = stub_->learn(&context, request, &reply);

  if (status.ok()) {
    return reply;
  } else {
    std::cerr << status.error_code() << ": " << status.error_message() << std::endl;
    abort();
  }
}

PaxosMsg
PaxosInvoker::get_vote(EpochT epoch, NodeIDT node_id) {
  PaxosMsg request, reply;
  request.set_epoch(epoch);
  request.set_node_id(node_id);
  ClientContext context;
  auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(PAXOS_VOTE_TIMEOUT);
  Status status = stub_->get_vote(&context, request, &reply);

  if (status.ok()) {
    return reply;
  } else {
    std::cerr << status.error_code() << ": " << status.error_message() << std::endl;
    abort();
  }
}


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

    auto ttt = mtx.try_lock();
    if (!ttt) {
      std::cout << "busy waiting" << std::endl;
      mtx.lock();
    }
    while (!has_valid_leader()) {
      std::cout << "not valid detected" << std::endl;
      mtx.unlock();
      while (!has_valid_leader())
        boost::this_fiber::yield();
      mtx.lock();
    }
    // if I'm leader
    auto inst_id = next_id.fetch_add(1, std::memory_order_acq_rel);
    auto epoch = view.epoch;
    auto self_id = view.self_id;
    c.first->epoch = view.epoch;
    c.first->id = inst_id;
    auto str = c.second;
    std::atomic<uint32_t> counter;
    std::atomic<uint32_t> finished;
    counter.store(1, std::memory_order_release);
    finished.store(1, std::memory_order_release);
    std::vector<boost::fibers::fiber> fbs;
    for (auto &invoker: invokers) {
      if (invoker.first != view.self_id) {
        boost::fibers::fiber f([=, &counter, &finished]() {
          auto commit_result =
            invoker.second->
              commit(view.epoch,
                     view.self_id,
                     inst_id,
                     &*str);
          finished.fetch_add(1, std::memory_order_release);
          if (commit_result.result() == PaxosMsg::SUCCESS)
            counter.fetch_add(1, std::memory_order_release);
        });
        fbs.push_back(std::move(f));
      }
    }
    PaxosRecord record;
    auto result = records.insert({inst_id, record});
    if (result.second) {
      result.first->second.promised_epoch = view.epoch;
      result.first->second.status = PREPARED;
      result.first->second.value = new std::string(*c.second);
      auto quorum_size = (view.vm.size() + 1) / 2;

      while (true) {
        boost::this_fiber::yield();
        auto t = counter.load(std::memory_order_acquire);
        if (t >= quorum_size)
          break;

        // TODO: check commit failure
        if (finished.load(std::memory_order_acquire) == view.vm.size()) {
          break;
        }
      }

      // if success
      {
        for (auto &f: fbs) {
          f.detach();
        }
      }

      for (auto& invoker: invokers) {
        if (invoker.first != view.self_id) {
          boost::fibers::fiber([=]() {
            invoker.second->
              learn(epoch,
                    self_id,
                    inst_id,
                    &*str);
            }).detach();
        }
      }
      // TODO: persistent
      records[inst_id].status = LEARNED;
      boost::this_fiber::yield();
      c.first->finish_propose(true);
      mtx.unlock();
      continue;
    } else {
      // if not leader, forward to leader
      mtx.unlock();
      invokers[view.leader_id]->propose(*c.second);
      c.first->finish_propose(true);
    }
  }
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
  next_id.store(1, std::memory_order_release);
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
                   const PaxosMsg* request,
                   PaxosMsg* result) {
  if (view.leader_id != view.self_id) {
    result->set_result(PaxosMsg::FAILURE);
    return Status::OK;
  }
  while (!context->IsCancelled()) {
    auto token = this->async_propose(request->value());
    while (!token->is_finished()) {
      std::this_thread::yield();
      if (context->IsCancelled())
        break;
    }
    if (token->is_finished() &&
        token->get_result()) {
      result->set_result(PaxosMsg::SUCCESS);
      return Status::OK;
    } else
      result->set_result(PaxosMsg::FAILURE);
  }
  return Status::CANCELLED;
}


Status
PaxosImpl::commit(grpc::ServerContext* context,
                  const PaxosMsg* request,
                  PaxosMsg* response) {
  if (context->IsCancelled()) {
    return Status::CANCELLED;
  }
  auto request_epoch = request->epoch();
  auto request_instance_id = request->instance_id();
  mtx.lock();
  // should consider if the node never
  // sees the prepare from new leader
  if (request_epoch >= view.epoch) {
    if (request_epoch > view.epoch) {
      view.epoch = request_epoch;
      view.leader_id = request->node_id();
    }
    auto record = PaxosRecord(PREPARED, request_epoch, nullptr);
    auto kv_pair = std::make_pair(request_instance_id, record);
    auto inst_res = records.insert(kv_pair);
    response->set_result(PaxosMsg::SUCCESS);
    if (inst_res.second) {
      if (request->value().length() != 0) {
        auto value = new string(request->value());
        inst_res.first->second.value = value;
      }
    } else {
      if (inst_res.first->second.status == LEARNED) {
        response->set_result(PaxosMsg::CONFLICT);
        goto commit_out;
      }
      inst_res.first->second.promised_epoch = kv_pair.second.promised_epoch;
      if (request->value().length() != 0) {
        if (inst_res.first->second.value == nullptr ||
            !bytes_eq(*inst_res.first->second.value, request->value())) {
          if (inst_res.first->second.value != nullptr)
            delete inst_res.first->second.value;
          string* value = new string;
          *value = request->value();
          inst_res.first->second.value = value;
        }
      }
    }
  } else {
    response->set_result(PaxosMsg::FOLLOWUP);
    response->set_node_id(view.leader_id);
  }
commit_out:
  mtx.unlock();
  return Status::OK;
}

Status
PaxosImpl::learn(grpc::ServerContext* context,
                 const PaxosMsg* request,
                 PaxosMsg* response) {
  auto request_epoch = request->instance_id();
  auto request_instance_id = request->instance_id();
  auto record = PaxosRecord(LEARNED, 0, nullptr);
  auto kv_pair = std::make_pair(request_instance_id, record);
  mtx.lock();
  auto inst_res = records.insert(kv_pair);
  response->set_result(PaxosMsg::SUCCESS);
  if (inst_res.second) {
    if (request->value().length() != 0) {
      auto value = new string;
      *value = request->value();
      inst_res.first->second.value = value;
    }
  } else {
    if (inst_res.first->second.status == LEARNED) {
      if (inst_res.first->second.value == nullptr) {
        if (request->value().length() != 0)
          response->set_result(PaxosMsg::CONFLICT);
      } else if (!bytes_eq(*inst_res.first->second.value, request->value()))
        response->set_result(PaxosMsg::CONFLICT);
      goto learn_out;
    } else {
      inst_res.first->second.status = LEARNED;
      inst_res.first->second.promised_epoch = request_epoch;
      if (request->value().length() != 0) {
        if (inst_res.first->second.value == nullptr ||
            !bytes_eq(*inst_res.first->second.value, request->value())) {
          if (inst_res.first->second.value != nullptr)
            delete inst_res.first->second.value;
          string* value = new string;
          *value = request->value();
          inst_res.first->second.value = value;
        }
      }
    }
  }
learn_out:
  mtx.unlock();
  return Status::OK;
}

Status
PaxosImpl::get_vote(grpc::ServerContext* context,
             const PaxosMsg* request,
             PaxosMsg* response) {
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

std::shared_ptr<ProposeToken>
PaxosImpl::async_propose(const string& value) {
  auto c = std::shared_ptr<string>(new string(value));
  auto token = std::shared_ptr<ProposeToken>(new ProposeToken());
  command_chan.push(std::make_pair(token, c));
  return token;
}

void
PaxosImpl::debug_request_leader() {
  while(true) {
    mtx.lock();
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
        boost::fibers::fiber f([=, &counter, &finished]() {
          auto vote_result =
            invoker.second->get_vote(view.epoch, view.self_id);
          finished.fetch_add(1, std::memory_order_release);
          if (vote_result.result() == PaxosMsg::SUCCESS)
            counter.fetch_add(1, std::memory_order_release);
        });
      }
    }
    while (true) {
      boost::this_fiber::yield();
      auto t = counter.load(std::memory_order_acquire);
      if (t >= quorum_size)
        break;

      // TODO: check commit failure
      if (finished.load(std::memory_order_acquire) == view.vm.size()) {
        break;
      }
    }
    // if success
    {
      for (auto &f: fbs)
        f.detach();
      mtx.lock();
      view.self_role = LEADER;
      view.leader_id = view.self_id;
      leader_valid.store(true, std::memory_order_acquire);
      // Prepare
      mtx.unlock();
      return;
    }
  }
}
