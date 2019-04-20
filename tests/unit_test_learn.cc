#include <vector>
#include <iostream>
#include <thread>
#include <boost/fiber/all.hpp>
#include <boost/thread/thread_pool.hpp>
#include "ipaxos.h"
using grpc::ServerBuilder;
using grpc::Server;

int main()
{
  PaxosNode::ViewMapT vm;
  std::cout.sync_with_stdio(false);
  vm[0] = {"localhost:40000", Role::LEADER};
  vm[1] = {"localhost:40001", Role::FOLLOWER};
  vm[2] = {"localhost:40003", Role::FOLLOWER};
  PaxosNode nodes[3];
  for (int i = 0;i < 3;++i) {
    nodes[i].init(0, i, vm);
  }

  for (int i = 0;i < 3;++i)
  {
    nodes[i].init_invokers();
  }


  boost::basic_thread_pool pool(4);
  for (int j = 0;j < 4;++j) {
  pool.submit([&]() {
    std::vector<boost::fibers::fiber> handles;
    for (int i = 0;i < 250;++i) {
      auto t = boost::fibers::fiber([=, &vm, &nodes](){
        ClientContext context;
        std::string e("0");
        nodes[0].propose(e);
      });
      handles.push_back(std::move(t));
    }
    for (auto &t: handles) {
      t.join();
    }
    std::cout << "finish" << std::endl;
  });
  }
  pool.close();


  for (int i = 0;i < 3;++i) {
    std::cout << "print node " << i << std::endl;
     // nodes[i].debug_print();
  }

  return 0;
}

