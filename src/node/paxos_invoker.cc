#include "node/paxos_impl.h"

std::pair<bool, std::shared_ptr<std::vector<PaxosMsg>>>
PaxosInvoker::propose(const std::vector<std::string>& value) {
  if (!check_state()) {
    return std::make_pair(false, nullptr);
  }

  assert(!value.empty());

  PaxosMsg request, reply;
  auto* replies = new std::vector<PaxosMsg>();
  ClientContext context;
  auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(PAXOS_COMMIT_TIMEOUT * 4);
  context.set_deadline(deadline);
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
    replies->push_back(reply);
  }
  // t.join();
  auto status = stream->Finish();

  // std::chrono::duration<double> diff = std::chrono::system_clock::now() - now;
  // std::cout << "latency: " << diff.count() << std::endl;
  if (status.ok()) {
    return std::make_pair(true, std::shared_ptr<std::vector<PaxosMsg>>(replies));
  } else {
    std::cerr << status.error_code() << ": " << status.error_message() << std::endl;
    if (do_check_state())
      abort();
    else
      return std::make_pair(false, nullptr);
  }
}

std::pair<bool, std::shared_ptr<std::vector<PaxosMsg>>>
PaxosInvoker::commit(EpochT epoch, NodeIDT node_id,
                           std::vector<InstanceIDT> instance_id,
                           const std::vector<std::string>& value) {
  if (!check_state()) {
    return std::make_pair(false, nullptr);
  }

  assert(instance_id.size() == value.size());
  assert(!instance_id.empty());

  PaxosMsg request, reply;
  auto replies = new std::vector<PaxosMsg>();
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
    replies->push_back(reply);
  }
  // t.join();


  auto status = stream->Finish();

  if (status.ok()) {
    return std::make_pair(true, std::shared_ptr<std::vector<PaxosMsg>>(replies));
  } else {
    std::cerr << status.error_code() << ": " << status.error_message() << std::endl;
    if (do_check_state())
      abort();
    else
      return std::make_pair(false, nullptr);
  }
}

std::pair<bool, std::shared_ptr<std::vector<PaxosMsg>>>
PaxosInvoker::learn(EpochT epoch, NodeIDT node_id,
                         std::vector<InstanceIDT> instance_id,
                         const std::vector<std::string>& value) {
  if (!check_state()) {
    return std::make_pair(false, nullptr);
  }
  assert(instance_id.size() == value.size());
  assert(!instance_id.empty());

  PaxosMsg request, reply;
  auto replies = new std::vector<PaxosMsg>();
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
    replies->push_back(reply);
  }
  // writer.join();

  auto status = stream->Finish();

  if (status.ok()) {
    return std::make_pair(true, std::shared_ptr<std::vector<PaxosMsg>>(replies));
  } else {
    std::cerr << status.error_code() << ": " << status.error_message() << std::endl;
    if (do_check_state())
      abort();
    else
      return std::make_pair(false, nullptr);
  }
}

std::pair<bool, PaxosMsg>
PaxosInvoker::get_vote(EpochT epoch, NodeIDT node_id) {
  if (!check_state()) {
    return std::make_pair(false, PaxosMsg());
  }

  PaxosMsg request, reply;
  request.set_epoch(epoch);
  request.set_node_id(node_id);
  ClientContext context;
  auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(PAXOS_VOTE_TIMEOUT);
  context.set_deadline(deadline);
  Status status = stub_->get_vote(&context, request, &reply);

  if (status.ok()) {
    return std::make_pair(true, reply);
  } else {
    std::cerr << status.error_code() << ": " << status.error_message() << std::endl;
    if (do_check_state())
      abort();
    else
      return std::make_pair(false, reply);
  }
}

std::pair<bool, std::shared_ptr<std::vector<PaxosMsg>>>
PaxosInvoker::ask_follow(EpochT epoch, NodeIDT node_id,
                         const std::vector<InstanceIDT>& ranges) {
  if (!check_state()) {
    return std::make_pair(false, nullptr);
  }

  PaxosMsg request, reply;
  auto replies = new std::vector<PaxosMsg>();
  ClientContext context;
  auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(PAXOS_COMMIT_TIMEOUT);
  request.set_epoch(epoch);
  request.set_node_id(node_id);
  for (auto& r: ranges)
    request.add_ranges(r);

  auto reader = stub_->ask_follow(&context, request);
  while (reader->Read(&reply))
    replies->push_back(reply);
  auto status = reader->Finish();

  if (status.ok()) {
    return std::make_pair(true, std::shared_ptr<std::vector<PaxosMsg>>(replies));
  } else {
    std::cerr << status.error_code() << ": " << status.error_message() << std::endl;
    if (do_check_state())
      abort();
    else
      return std::make_pair(false, nullptr);
  }
}


