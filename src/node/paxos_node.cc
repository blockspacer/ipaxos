#include "node/paxos_node.h"

bool PaxosNode::propose(const string& value) {
  auto token = async_propose(value);
  while (!token->paxos_commit_finished()) {};
  return token->get_paxos_commit_result();
}

std::shared_ptr<ProposeToken>
PaxosNode::async_propose(const string& value) {
  auto p = impl.async_propose(value);
  return p;
}
