#include <vector>
#include <iostream>
#include <thread>
#include <boost/fiber/all.hpp>
#include <boost/thread/thread_pool.hpp>
#include "ipaxos.h"
using grpc::ServerBuilder;
using grpc::Server;
#define NODE_SIZE 3

int main()
{
  PaxosView::ViewMapT vm;
  vm[0] = "localhost:40000";
  vm[1] = "localhost:40001";
  vm[2] = "localhost:40002";
  PaxosRole roles[NODE_SIZE] = {PaxosRole::LEADER,
      PaxosRole::FOLLOWER, PaxosRole::FOLLOWER};
  PaxosNode nodes[NODE_SIZE];
  auto config = PaxosConfig();

  for (int i = 0;i < NODE_SIZE;++i) {
    PaxosView view;
    view.init(1, i, 0, roles[i], vm);
    nodes[i].init(view, config);
  }

  for (int i = 0;i < NODE_SIZE;++i)
  {
    nodes[i].init_invokers();
  }

  nodes[0].stop();


  auto start = std::chrono::system_clock::now();
  auto thread_count = std::thread::hardware_concurrency();
  boost::basic_thread_pool pool(thread_count);
  for (int j = 0;j < 2;++j) {
    pool.submit([&, j]() {
      std::vector<boost::fibers::fiber> handles;
      for (int i = 0;i < 10000;++i) {
        auto e = std::to_string(i);
        auto t = boost::fibers::fiber([&, j, e](){
          ClientContext context;
          auto t = nodes[j+1].async_propose(e);
          t->wait_paxos_commit();
        });
        handles.push_back(std::move(t));
      }
      for (auto &t: handles) {
        t.join();
      }
      handles.clear();
    });
  }

  pool.close();
  pool.join();

  auto diff = std::chrono::system_clock::now() - start;
  std::cout << static_cast<double>(diff.count()) / 1000000000 << std::endl;


  auto b = nodes[1].debug_record();
  auto c = nodes[2].debug_record();
  for (auto& kv:b)
    assert(c[kv.first] == kv.second);

  for (int i = 0;i < NODE_SIZE;++i)
     nodes[i].stop();

  return 0;
}

