#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>

namespace tensorpipe {

template <typename T>
class Queue {
 public:
  explicit Queue(int capacity = 1) : capacity_(capacity) {}

  void push(T t) {
    std::unique_lock<std::mutex> lock(mutex_);
    while (items_.size() >= capacity_) {
      cv_.wait(lock);
    }
    items_.push_back(std::move(t));
    cv_.notify_all();
  }

  T pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (items_.size() == 0) {
      cv_.wait(lock);
    }
    T t(std::move(items_.front()));
    items_.pop_front();
    cv_.notify_all();
    return t;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  const int capacity_;
  std::deque<T> items_;
};

} // namespace tensorpipe
