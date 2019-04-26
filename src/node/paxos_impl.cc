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
  auto v = new google::protobuf::BytesValue;
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


void PaxosImpl::handle_proposals() {
  while (true) {
    CommandT c;
    while (!command_chan.try_pop(c)) {}
    // if I'm leader
    if (view.self_id == view.leader_id) {
      auto inst_id = next_id.fetch_add(1, std::memory_order_acq_rel);
      auto epoch = view.epoch;
      auto self_id = view.self_id;
      c.first->epoch = view.epoch;
      c.first->id = inst_id;
      auto str = c.second;
      std::atomic<uint32_t> counter;
      counter.store(1, std::memory_order_release);
      std::vector<boost::fibers::fiber> fbs;
      for (auto &invoker: invokers) {
        if (invoker.first != view.self_id) {
          boost::fibers::fiber f([=, &counter]() {
            auto commit_result =
              invoker.second->
                commit(view.epoch,
                       view.self_id,
                       inst_id,
                       &*str);
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
        }
        for (auto &f: fbs) {
          f.detach();
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
        continue;
      } else {
        abort();
      }
    } else {
      // if not leader, forward to leader
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
            v.second.first,
            grpc::InsecureChannelCredentials()));
      invokers.insert({v.first, invoker});
    }
  }
}

bool
PaxosImpl::init(PaxosView&& view_, PaxosConfig&& config_) {
    status.store(STABLE, std::memory_order_release);
    next_id.store(1, std::memory_order_release);
    view = view_;
    config = config_;
    _waiting_handler = std::thread([=]() {
      ServerBuilder builder;
      builder.AddListeningPort(view.vm[view.self_id].first, grpc::InsecureServerCredentials());
      builder.RegisterService(this);
      auto service = std::move(builder.BuildAndStart());
      service->Wait();
    });
    _receiving_handler = std::thread(std::bind(&PaxosImpl::handle_proposals, this));
    return true;
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
  if (status.load(std::memory_order_acquire) == STABLE) {
    mtx.lock();
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
      // update existing entry
      if (kv_pair.second.promised_epoch > inst_res.first->second.promised_epoch) {
        inst_res.first->second.status = PREPARED;
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
      } else if (kv_pair.second.promised_epoch < inst_res.first->second.promised_epoch){
        response->set_result(PaxosMsg::FOLLOWUP);
        response->set_node_id(view.leader_id);
      }
    }
    mtx.unlock();
  } else {
    // TODO: not implemented
    GPR_ASSERT(0);
  }
  return Status::OK;
}

Status
PaxosImpl::learn(grpc::ServerContext* context,
                 const PaxosMsg* request,
                 PaxosMsg* response) {
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
      response->set_result(PaxosMsg::CONFLICT);
    } else {
      inst_res.first->second.status = LEARNED;
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
  mtx.unlock();
  return Status::OK;
}

Status
PaxosImpl::propose(grpc::ServerContext* context,
                   const PaxosMsg* request,
                   PaxosMsg* result) {
  while (!context->IsCancelled()) {
    auto token = this->async_propose(request->value());
    while (!token->is_finished()) {
      std::this_thread::yield();
      if (context->IsCancelled())
        break;
      if (token->get_result()) {
        result->set_result(PaxosMsg::SUCCESS);
        return Status::OK;
      }
    }
  }
  return Status::CANCELLED;
}

std::shared_ptr<ProposeToken>
PaxosImpl::async_propose(const string& value) {
  auto c = std::shared_ptr<string>(new string(value));
  auto token = std::shared_ptr<ProposeToken>(new ProposeToken());
  command_chan.push(std::make_pair(token, c));
  return token;
}
