#ifndef _PAXOS_IMPL_H_
#define _PAXOS_IMPL_H_
#include <thread>
#include <grpcpp/grpcpp.h>
#include "node/paxos_config.h"
#include "ipaxos_rpc.grpc.pb.h"
#include <utility>
#include <memory>
#include <boost/fiber/all.hpp>
#include <boost/thread/thread.hpp>
#include <tbb/concurrent_queue.h>

using ipaxos::Paxos;
using ipaxos::PaxosMsg;
using grpc::Channel;
using grpc::Status;
using grpc::ClientContext;
using grpc::ServerContext;
using grpc::ServerBuilder;
using grpc::ChannelInterface;

enum PaxosRole {
  LEADER,
  FOLLOWER,
  CANDIDATE
};

enum InstanceStatus {
  EMPTY,    // empty record
  PREPARED,
  LEARNED,
  APPLIED
};

typedef uint64_t NodeIDT;
typedef uint64_t InstanceIDT;
typedef uint64_t EpochT;


class ProposeToken;
class PaxosView;
class PaxosInvoker;
class PaxosImpl;

typedef std::pair<
          std::vector<std::shared_ptr<ProposeToken>>,
          std::shared_ptr<std::vector<std::string>>> CommandT;
typedef tbb::concurrent_queue<CommandT> PaxosChanT;

// Use Acquire-Release ordering to sysnchronize
// at the point of loading of *finish*
class ProposeToken {
  friend class PaxosImpl;
public:
  ProposeToken() {
      finish.store(false, std::memory_order_release);
    }

  inline bool is_finished() {
    return finish.load(std::memory_order_acquire);
  }

  inline void wait() {
    while (!finish.load(std::memory_order_acquire))
      boost::this_fiber::yield();
  }

  inline bool get_result() {
    return success;
  }

  inline bool has_id() {
    return id != 0;
  }

private:
  void finish_propose(bool result) {
    success = true;
    finish.store(true, std::memory_order_release);
  }
  InstanceIDT id = 0;
  EpochT epoch = 0;
  bool success = false;
  std::atomic<bool> finish;
};

class PaxosView {
public:
  PaxosView() = default;

  typedef std::map<NodeIDT, std::string> ViewMapT;
  bool init(EpochT epoch_, NodeIDT self_id_, NodeIDT leader_id_,
            PaxosRole self_role_, const ViewMapT& vm_) {
    assert(epoch_ != 0);
    epoch = epoch_;
    vm = vm_;
    self_id = self_id_;
    self_role = self_role_;
    leader_id = leader_id_;
    std::cout << "self_id: " << self_id_ << std::endl;
    return true;
  }


  NodeIDT leader_id;
  NodeIDT self_id;
  PaxosRole self_role;
  EpochT epoch;
  ViewMapT vm;
};

class PaxosInvoker {
public:
  PaxosInvoker(std::shared_ptr<Channel> channel,
               uint32_t timeout)
    : stub_(Paxos::NewStub(channel)),
      connection_check_timeout(timeout) {
      chan_ = channel;
    check_point = std::chrono::system_clock::now() + std::chrono::seconds(connection_check_timeout);
  }

  std::pair<bool, std::shared_ptr<std::vector<PaxosMsg>>>
  commit(EpochT epoch, NodeIDT node_id,
         std::vector<InstanceIDT> instance_id,
         const std::vector<std::string>& value,
         bool compact_and_check = false);

  std::pair<bool, std::shared_ptr<std::vector<PaxosMsg>>>
  learn(EpochT epoch, NodeIDT node_id,
        std::vector<InstanceIDT> instance_id,
        const std::vector<std::string>& value);

  std::pair<bool, std::shared_ptr<std::vector<PaxosMsg>>>
  propose(const std::vector<std::string>& value);

  std::pair<bool, std::shared_ptr<std::vector<PaxosMsg>>>
  ask_follow(EpochT epoch, NodeIDT node_id,
             const std::vector<InstanceIDT>& ranges);

  std::pair<bool, PaxosMsg> get_vote(EpochT epoch, NodeIDT node_id);

private:
  inline bool check_state() {
    auto now = std::chrono::system_clock::now();
    if (now > check_point) {
      check_point = now + std::chrono::seconds(connection_check_timeout);
      return do_check_state();
    } else
      return online;
  }

  inline bool do_check_state() {
    auto state = chan_->GetState(true);
    check_point = std::chrono::system_clock::now() + std::chrono::seconds(connection_check_timeout);
    if (state == GRPC_CHANNEL_READY || state == GRPC_CHANNEL_IDLE) {
      online = true;
      return true;
    } else {
      online = false;
      return false;
    }
  }


  std::chrono::time_point<std::chrono::system_clock> check_point;
  uint32_t connection_check_timeout = 0xFFFFFFF;
  bool online = true;
  std::shared_ptr<ChannelInterface> chan_;
  std::unique_ptr<Paxos::Stub> stub_;
};



class PaxosImpl final : public Paxos::Service {
  class PaxosLearnedIndex;
public:
  class PaxosRecord;
  bool init(PaxosView&& view, PaxosConfig&& config);
  void init_invokers();
  std::shared_ptr<ProposeToken> async_propose(const std::string &);
  // std::shared_ptr<ProposeToken> async_propose(const std::vector<std::string> &);

  inline bool is_leader() { return view.self_id == view.leader_id; };

  bool commit_existed();

  bool learn_existed();

  Status commit(grpc::ServerContext* context,
                grpc::ServerReaderWriter<PaxosMsg, PaxosMsg>* stream) override;

  // learn always succeeds
  Status learn(grpc::ServerContext* context,
                grpc::ServerReaderWriter<PaxosMsg, PaxosMsg>* stream) override;

  // start leader election
  Status get_vote(grpc::ServerContext* context,
               const PaxosMsg* request,
               PaxosMsg* response) override;

  Status propose(grpc::ServerContext* context,
                grpc::ServerReaderWriter<PaxosMsg, PaxosMsg>* stream) override;

  Status ask_follow(grpc::ServerContext* context,
                    const PaxosMsg* request,
                    grpc::ServerWriter<PaxosMsg>* writer) override;



  inline bool has_valid_leader() {
    return leader_valid.load(std::memory_order_acquire);
  }

  // true if become leader
  bool request_for_leader();

  // loop for stable leader
  void leader_election();

  std::map<InstanceIDT, PaxosRecord> debug_record() {
    mtx.lock();
    return records;
    mtx.unlock();
  }

  void debug_print() {
    mtx.lock();
    for (auto& v : records) {
      std::cout << v.first << " : " << v.second.value << " : "
        << v.second.status << std::endl;
    }
    mtx.unlock();
  }
  class PaxosRecord {
    friend class PaxosImpl;
  public:
    PaxosRecord() = default;
    PaxosRecord(InstanceStatus status, EpochT epoch,
           const std::string& value) :
           status(status),
           promised_epoch(epoch),
           value(value) {}
    bool operator==(const PaxosRecord& rhs) {
      return value == rhs.value;
    }
    ~PaxosRecord() {
    }

  private:
    InstanceStatus status = EMPTY;
    EpochT promised_epoch = 0;
    std::string value;
  };

  // this class record the position of learned records
private:
  class PaxosLearnedIndex{
    friend class PaxosImpl;
  public:
    PaxosLearnedIndex() = default;
    PaxosLearnedIndex(const PaxosRecord& record) {
      value = new std::string;
      *value = record.value;
    }
    PaxosLearnedIndex(PaxosRecord&& record) {
      value = new std::string;
      std::swap(record.value, *value);
    }

    std::string& get_value() {
      if (value != nullptr) {
        return *value;
      } else {
        // TODO: load value
      }
    }
  private:
    std::string* value;
    uint64_t begin;
    uint64_t length;
  };

  // proposing loop
  void handle_proposals();
  std::vector<InstanceIDT> get_learned_ranges();
  std::vector<InstanceIDT> get_known_ranges();
  InstanceIDT compact();

  boost::fibers::mutex mtx;

  // record empty holes
  std::map<InstanceIDT, PaxosRecord> records;

  // moved after it is applied
  std::vector<PaxosLearnedIndex> compacted_records;

  std::thread _waiting_handler;
  std::thread _receiving_handler;

  InstanceIDT next_id;
  std::atomic<bool> leader_valid;
  std::atomic<bool> running_for_leader;

  uint32_t lease_timeout;

  // record largest voted epoch
  EpochT voted_epoch = 0;
  InstanceIDT voted_for = 0;

  PaxosChanT command_chan;

  PaxosView view;
  PaxosConfig config;
  std::map<NodeIDT, PaxosInvoker*> invokers;
};


#endif
