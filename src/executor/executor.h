#ifndef _PAXOS_EXECUTOR_H_
#define _PAXOS_EXECUTOR_H_
#include "../node/paxos_node.h"
class Executor {
public:
  virtual void execute(std::string value);
};

template <class T, class U>
class MapExecutor : public Executor {
public:
  void execute(std::string value) override;
private:
  std::map<T, U> map_;
};
#endif
