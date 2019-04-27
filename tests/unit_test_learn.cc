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
  PaxosView::ViewMapT vm;
  vm[0] = "localhost:40000";
  vm[1] = "localhost:40001";
  vm[2] = "localhost:40002";
  PaxosRole roles[3] = {PaxosRole::LEADER, PaxosRole::FOLLOWER, PaxosRole::FOLLOWER};
  PaxosNode nodes[3];
  for (int i = 0;i < 3;++i) {
    PaxosView view;
    view.init(1, i, 0, roles[i], vm);
    nodes[i].init(view, PaxosConfig());
  }

  for (int i = 0;i < 3;++i)
  {
    nodes[i].init_invokers();
  }


  auto thread_count = std::thread::hardware_concurrency();
  boost::basic_thread_pool pool(thread_count);
  for (int j = 0;j < 3;++j) {
    pool.submit([&, j]() {
      boost::fibers::use_scheduling_algorithm<boost::fibers::algo::work_stealing>(thread_count);
      std::vector<boost::fibers::fiber> handles;
      for (int i = 0;i < 333;++i) {
        auto e = std::to_string(i);
        auto t = boost::fibers::fiber([&, j, e](){
          ClientContext context;
          auto t = nodes[j].async_propose(e);
          t->wait();
        });
        handles.push_back(std::move(t));
      }
      for (auto &t: handles) {
        t.join();
      }
    });
  }
  pool.close();
  pool.join();


  for (int i = 0;i < 3;++i) {
    std::cout << "print node " << i << std::endl;
    // nodes[i].debug_print();
  }

  return 0;
}

