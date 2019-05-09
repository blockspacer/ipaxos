#ifndef _COLLECT_H_
#define _COLLECT_H_
#include <atomic>
#include <memory>
template<typename T>
class Collect {
public:
  Collect() {
    finish.store(false, std::memory_order_release);
  }
  bool has_value() { return finish.load(std::memory_order_acquire); }
  void collect(std::shared_ptr<T>&& p) {
    ptr.swap(p);
    finish.store(true, std::memory_order_release);
  }
  T* operator->() { return ptr.operator->(); }
private:
  std::shared_ptr<T> ptr;
  std::atomic<bool> finish;
};
#endif
