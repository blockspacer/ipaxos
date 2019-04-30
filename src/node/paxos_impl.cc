#include "paxos_impl.h"
#include "../common/utils.h"

std::vector<PaxosMsg>
PaxosInvoker::propose(const std::vector<std::string>& value) {
  PaxosMsg request, reply;
  std::vector<PaxosMsg> replies;
  ClientContext context;
  auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(PAXOS_COMMIT_TIMEOUT * 2);
  // context.set_deadline(deadline);
  // auto now = std::chrono::system_clock::now();
  auto stream = stub_->propose(&context);

  // auto t = boost::thread([&](){
    for (auto &s: value) {
      request.set_value(s);
      stream->Write(request);
    }
    stream->WritesDone();
  // });

  while (stream->Read(&reply)) {
    replies.push_back(reply);
  }
  // t.join();
  auto status = stream->Finish();

  // std::chrono::duration<double> diff = std::chrono::system_clock::now() - now;
  // std::cout << "latency: " << diff.count() << std::endl;
  if (status.ok()) {
    return replies;
  } else {
    std::cerr << status.error_code() << ": " << status.error_message() << std::endl;
    abort();
  }
}

std::vector<PaxosMsg>
PaxosInvoker::commit(EpochT epoch, NodeIDT node_id,
                           std::vector<InstanceIDT> instance_id,
                           const std::vector<std::string>& value) {
  PaxosMsg request, reply;
  std::vector<PaxosMsg> replies;
  request.set_epoch(epoch);
  request.set_node_id(node_id);

  ClientContext context;
  auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(PAXOS_COMMIT_TIMEOUT);
  context.set_deadline(deadline);
  auto stream = stub_->commit(&context);

  //auto t = boost::thread([&](){
    int iter = 0;
    for (auto &s: value) {
      request.set_instance_id(instance_id[iter]);
      request.set_value(s);
      stream->Write(request);
      iter++;
    }
    stream->WritesDone();
  // });

  for (auto &s: value) {
    stream->Read(&reply);
    replies.push_back(reply);
  }
  // t.join();


  auto status = stream->Finish();

  if (status.ok()) {
    return replies;
  } else {
    std::cerr << status.error_code() << ": " << status.error_message() << std::endl;
    abort();
  }
}

std::vector<PaxosMsg>
PaxosInvoker::learn(EpochT epoch, NodeIDT node_id,
                         std::vector<InstanceIDT> instance_id,
                         const std::vector<std::string>& value) {
  PaxosMsg request, reply;
  std::vector<PaxosMsg> replies;
  request.set_epoch(epoch);
  request.set_node_id(node_id);

  ClientContext context;
  auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(PAXOS_LEARN_TIMEOUT);
  context.set_deadline(deadline);
  auto stream = stub_->learn(&context);

  //auto writer = boost::thread([&](){
    int iter = 0;
    for (auto s: value) {
      request.set_instance_id(instance_id[iter]);
      request.set_value(s);
      stream->Write(request);
      iter++;
    }

    stream->WritesDone();
  // });

  for (auto &s: value) {
    stream->Read(&reply);
    replies.push_back(reply);
  }
  // writer.join();

  auto status = stream->Finish();

  if (status.ok()) {
    return replies;
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
    auto strs = c.second;
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
                     {inst_id},
                     *strs);
          finished.fetch_add(1, std::memory_order_release);
          if (commit_result[0].result() == PaxosMsg::SUCCESS)
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
      result.first->second.value = (*c.second)[0];
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
                    {inst_id},
                    *strs);
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
  mtx.lock();
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
    } else {
      reply.set_result(PaxosMsg::FOLLOWUP);
      reply.set_node_id(view.leader_id);
      std::cout << "???" << std::endl;
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
  mtx.lock();
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
  std::vector<string> values = {value};
  auto c = std::make_shared<std::vector<string>>(std::move(values));
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
