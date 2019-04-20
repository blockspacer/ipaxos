#include "node/paxos_node.h"

PaxosMsg
PaxosInvoker::commit(EpochT epoch, NodeIDT node_id,
                           InstanceIDT instance_id,
                           const std::string* value) {
  PaxosMsg request, reply;
  if (value != nullptr) {
    auto v = new google::protobuf::BytesValue;
    v->set_value(*value);
    request.set_allocated_value(v);
  }
  request.set_epoch(epoch);
  request.set_node_id(node_id);
  request.set_instance_id(instance_id);
  ClientContext context;
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
  if (value != nullptr) {
    auto v = new google::protobuf::BytesValue;
    v->set_value(*value);
    request.set_allocated_value(v);
  }
  request.set_epoch(epoch);
  request.set_node_id(node_id);
  request.set_instance_id(instance_id);
  auto v = new google::protobuf::BytesValue;
  ClientContext context;
  Status status = stub_->commit(&context, request, &reply);

  if (status.ok()) {
    return reply;
  } else {
    std::cerr << status.error_code() << ": " << status.error_message() << std::endl;
    abort();
  }
}

bool PaxosNode::propose(const string& value) {
  auto token = async_propose(value);
  return token.result();
}

PaxosNode::ProposeToken
PaxosNode::async_propose(const string& value) {
  if (view.leader_id == view.self_id) {
    boost::fibers::packaged_task<bool()> task([=](){
      auto inst_id = next_id.fetch_add(1, std::memory_order_acq_rel);
      int counter = 1;

      std::vector<boost::fibers::fiber> handles;
      for (auto node: view.vm) {
        if (node.first != view.self_id) {
          auto f = boost::fibers::fiber([=](){
            PaxosNode::Invokers::const_accessor a;
            invokers.find(a, node.first);
            auto resp = a->second->commit(
                          view.epoch,
                          view.self_id,
                          inst_id,
                          &value);
            a.release();
          });
          handles.push_back(std::move(f));
        }
      }

      typename PaxosNode::Records::accessor a;
      records.insert(a, inst_id);
      a->second.status = PREPARED;
      a->second.value = new string(value);
      a.release();
      for (auto &h: handles)
        h.join();
      return true;
    });
    auto token = ProposeToken(std::move(task.get_future()));
    boost::fibers::fiber(std::move(task)).detach();
    return token;
  }
}

void
PaxosNode::init_invokers() {
  for (const auto&v : view.vm) {
    if (v.first != view.self_id) {
      auto invoker = new PaxosInvoker(
          grpc::CreateChannel(
            v.second.first,
            grpc::InsecureChannelCredentials()));
      Invokers::accessor a;
      invokers.insert(a, v.first);
      a->second = invoker;
      a.release();
    }
  }
}

bool
PaxosImpl::init(const std::string& port, PaxosNode* node) {
    ptr = node;
    _waiting_handler = std::thread([=]() {
      ServerBuilder builder;
      builder.AddListeningPort(port, grpc::InsecureServerCredentials());
      builder.RegisterService(this);
      auto service = std::move(builder.BuildAndStart());
      service->Wait();
    });
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
  if (ptr->status.load(std::memory_order_acquire) == STABLE) {
    typename PaxosNode::Records::accessor a;
    auto record = PaxosNode::Record(PREPARED, request_epoch, nullptr);
    auto kv_pair = std::make_pair(request_instance_id, record);
    auto success = ptr->records.insert(a, kv_pair);
    response->set_result(PaxosMsg::SUCCESS);
    if (success) {
      if (request->has_value()) {
        auto value = new string;
        *value = request->value().value();
        a->second.value = value;
      }
    } else {
      // update existing entry
      if (kv_pair.second.promised_epoch > a->second.promised_epoch) {
        a->second.status = kv_pair.second.status;
        a->second.promised_epoch = kv_pair.second.promised_epoch;
        if (request->has_value()) {
          if (a->second.value == nullptr ||
              !bytes_eq(*a->second.value, request->value().value())) {
            if (a->second.value != nullptr)
              delete a->second.value;
            string* value = new string;
            *value = request->value().value();
            a->second.value = value;
          }
        }
      } else if (kv_pair.second.promised_epoch < a->second.promised_epoch){
        response->set_result(PaxosMsg::FOLLOWUP);
        response->set_node_id(ptr->view.leader_id);
      }
    }

    a.release();
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
  typename PaxosNode::Records::accessor a;
  auto request_instance_id = request->instance_id();
  auto record = PaxosNode::Record(LEARNED, 0, nullptr);
  auto kv_pair = std::make_pair(request_instance_id, record);
  auto success = ptr->records.insert(a, kv_pair);
  response->set_result(PaxosMsg::SUCCESS);
  if (success) {
    if (request->has_value()) {
      auto value = new string;
      *value = request->value().value();
      a->second.value = value;
    }
  } else {
    if (a->second.status == LEARNED) {
      response->set_result(PaxosMsg::CONFLICT);
    } else {
      a->second.status = LEARNED;
      if (request->has_value()) {
        if (a->second.value == nullptr ||
            !bytes_eq(*a->second.value, request->value().value())) {
          if (a->second.value != nullptr)
            delete a->second.value;
          string* value = new string;
          *value = request->value().value();
          a->second.value = value;
        }
      }
    }
  }
  return Status::OK;
}

Status
PaxosImpl::propose(grpc::ServerContext* context,
                   const ProposeRequest* request,
                   ProposeResult* result) {
  ptr->propose(request->value());
  result->set_success(true);
  return Status::OK;
}
