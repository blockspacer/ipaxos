#include "node/paxos_node.h"

bool PaxosNode::propose(const string& value) {
  auto token = async_propose(value);
  while (!token->is_finished()) {};
  return token->get_result();
}

// TODO: forward propose
std::shared_ptr<ProposeToken>
PaxosNode::async_propose(const string& value) {
  if (impl.is_leader()) {
    auto p = impl.async_propose(value);
    return p;
  } else {
    // impl.forward_propose(value);
  }
}
