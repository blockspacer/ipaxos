#ifndef _PAXOS_NODE_H_
#define _PAXOS_NODE_H_
#include <atomic>
#include <list>
#include <map>
#include <utility>
#include <thread>
#include <memory>
#include "../common/utils.h"
#include "paxos_impl.h"
#include <boost/fiber/all.hpp>
using std::string;

class PaxosNode;

class PaxosNode {
public:
  PaxosNode() = default;
  void init(PaxosView view, PaxosConfig config) {
    bool result = impl.init(std::move(view), std::move(config));
    inited = true;
  }

  void init_invokers() { impl.init_invokers(); };


  std::shared_ptr<ProposeToken> async_propose(const string& value);
  bool propose(const string& value);

  void debug_print() {
    impl.debug_print();
  }


  inline bool is_leader() { return impl.is_leader(); }

private:
  bool inited = false;
  PaxosImpl impl;
  PaxosChanT chan;
};
#endif
