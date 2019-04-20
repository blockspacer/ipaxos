#ifndef _PAXOS_NODE_H_
#define _PAXOS_NODE_H_
#include <atomic>
#include <list>
#include <map>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "ipaxos_rpc.grpc.pb.h"
#include "../common/utils.h"
#include <tbb/concurrent_hash_map.h>
#include <boost/fiber/all.hpp>

using std::string;
using ipaxos::Paxos;
using ipaxos::PaxosMsg;
using ipaxos::ProposeRequest;
using ipaxos::ProposeResult;
using grpc::Channel;
using grpc::Status;
using grpc::ClientContext;
using grpc::ServerContext;
using grpc::ServerBuilder;

enum Role {
  LEADER,
  FOLLOWER
};

enum InstanceStatus {
  EMPTY,    // empty record
  PREPARED,
  LEARNED,
  APPLIED
};

enum NodeStatus {
  STABLE,
  VIEWCHANGING,
  FOLLOWUP
};

typedef uint64_t NodeIDT;
typedef uint64_t InstanceIDT;
typedef uint64_t EpochT;

class PaxosInvoker;
class PaxosImpl;
class PaxosNode;

class PaxosInvoker {
public:
  PaxosInvoker(std::shared_ptr<Channel> channel)
    : stub_(Paxos::NewStub(channel)) {}

  PaxosMsg commit(EpochT epoch, NodeIDT node_id,
                  InstanceIDT instance_id,
                  const std::string* value);

  PaxosMsg learn(EpochT epoch, NodeIDT node_id,
                 InstanceIDT instance_id,
                 const std::string* value);

private:
  std::unique_ptr<Paxos::Stub> stub_;
};




class PaxosImpl final : public Paxos::Service {
public:
  bool init(const std::string& port, PaxosNode* node);
  Status commit(grpc::ServerContext* context,
                const PaxosMsg* request,
                PaxosMsg* response) override;

  // learn always succeeds
  Status learn(grpc::ServerContext* context,
               const PaxosMsg* request,
               PaxosMsg* response) override;

  Status propose(grpc::ServerContext* context,
                 const ProposeRequest* request,
                 ProposeResult* result) override;
private:
  PaxosNode* ptr = nullptr;
  std::thread _waiting_handler;
};


class PaxosNode final : public Paxos::Service {
public:
  typedef std::map<NodeIDT, std::pair<std::string, Role>> ViewMapT;
  friend class PaxosImpl;
  PaxosNode() = default;
  void init(EpochT epoch, NodeIDT self_id, const ViewMapT& vm) {
    bool result = view.init(epoch, self_id, vm);
    bool result2 = impl.init(view.vm.at(view.self_id).first, this);
    inited = true;
    status.store(STABLE, std::memory_order_release);
    next_id.store(0, std::memory_order_release);
  }

  void init_invokers();

  class ProposeToken {
  public:
    friend class PaxosNode;
    ProposeToken(boost::fibers::future<bool> f)
      : checker(std::move(f)) {}
    inline bool is_ready() {
      return checker.wait_for(std::chrono::seconds(0)) ==  boost::fibers::future_status::ready;
    }
    bool result() {
      return checker.get();
    }
  private:
    boost::fibers::future<bool> checker;
  };

  ProposeToken async_propose(const string& value);
  bool propose(const string& value);

  void debug_print() {
    for (auto v : records) {
      std::cout << v.first << " : " << *v.second.value << std::endl;
    }
  }
private:


  // doesn't need to be
  class View {
    friend class PaxosNode;
    friend class PaxosImpl;
    View() = default;

    bool init(EpochT epoch_, NodeIDT self_id_, const ViewMapT& vm_) {
      epoch = epoch_;
      vm = vm_;
      self_id = self_id_;
      std::cout << "self_id: " << self_id_ << std::endl;
      for(const auto& kv : vm)
        if (kv.second.second == LEADER) {
          leader_id = kv.first;
          return true;
        }
      return false;
    }


    NodeIDT leader_id;
    NodeIDT self_id;
    EpochT epoch;
    ViewMapT vm;
  };

  class Record {
    friend class PaxosNode;
    friend class PaxosImpl;
  public:
    Record() = default;
    Record(InstanceStatus status, EpochT epoch,
           std::string* value = nullptr) :
           status(status),
           promised_epoch(epoch),
           value(value) {}
    ~Record() {
      if (value != nullptr)
        delete value;
    }

  private:
    InstanceStatus status = EMPTY;
    EpochT promised_epoch = 0;
    string* value = nullptr;
  };

  typedef tbb::concurrent_hash_map<InstanceIDT, Record> Records;
  typedef tbb::concurrent_hash_map<InstanceIDT, PaxosInvoker*> Invokers;


  inline bool is_leader() { return view.leader_id = view.self_id; }

  bool inited = false;
  Records records;
  View view;
  Role role;
  Invokers invokers;
  std::atomic<NodeStatus> status;
  std::atomic<InstanceIDT> next_id;
  PaxosImpl impl;
};
#endif
