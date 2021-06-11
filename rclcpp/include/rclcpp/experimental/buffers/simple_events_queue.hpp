// Copyright 2021 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RCLCPP__EXPERIMENTAL__BUFFERS__SIMPLE_EVENTS_QUEUE_HPP_
#define RCLCPP__EXPERIMENTAL__BUFFERS__SIMPLE_EVENTS_QUEUE_HPP_

#include <mutex>
#include <queue>
#include <utility>

#include "rclcpp/experimental/buffers/events_queue.hpp"

namespace rclcpp
{
namespace experimental
{
namespace buffers
{

/**
 * @brief This class implements an EventsQueue as a simple wrapper around a std::queue.
 * It does not perform any checks about the size of queue, which can grow
 * unbounded without being pruned.
 * The simplicity of this implementation makes it suitable for optimizing CPU usage.
 */
class SimpleEventsQueue : public EventsQueue
{
public:
  RCLCPP_PUBLIC
  ~SimpleEventsQueue() override = default;

  /**
   * @brief enqueue event into the queue
   * Thread safe
   * @param event The event to enqueue into the queue
   */
  RCLCPP_PUBLIC
  void
  enqueue(const rclcpp::executors::ExecutorEvent & event) override
  {
    rclcpp::executors::ExecutorEvent single_event = event;
    single_event.num_events = 1;
    {
      std::unique_lock<std::mutex> lock(this->push_mutex_);
      for (size_t ev = 1; ev <= event.num_events; ev++ ) {
        event_queue_.push(single_event);
      }
    }
    events_queue_cv_.notify_one();
  }

  /**
   * @brief dequeue the front event from the queue.
   * The event is removed from the queue after this operation.
   * Callers should make sure the queue is not empty before calling.
   * Thread safe
   *
   * @return the front event
   */
  RCLCPP_PUBLIC
  rclcpp::executors::ExecutorEvent
  dequeue() override
  {
    std::unique_lock<std::mutex> lock(this->push_mutex_);
    rclcpp::executors::ExecutorEvent event = event_queue_.front();
    event_queue_.pop();
    return event;
  }

  /**
   * @brief Test whether queue is empty
   * Thread safe
   * @return true if the queue's size is 0, false otherwise.
   */
  RCLCPP_PUBLIC
  bool
  empty() const override
  {
    std::unique_lock<std::mutex> lock(this->push_mutex_);
    return event_queue_.empty();
  }

  /**
   * @brief Returns the number of elements in the queue.
   * Thread safe
   * @return the number of elements in the queue.
   */
  RCLCPP_PUBLIC
  size_t
  size() const override
  {
    std::unique_lock<std::mutex> lock(this->push_mutex_);
    return event_queue_.size();
  }

  /**
   * @brief waits for an event until timeout, gets a single event
   * Thread safe
   * @return true if event, false if timeout
   */
  RCLCPP_PUBLIC
  bool
  dequeue(
    rclcpp::executors::ExecutorEvent & event,
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds::max()) override
  {
    auto has_event_predicate = [this]() {return !event_queue_.empty();};

    std::unique_lock<std::mutex> lock(this->push_mutex_);

    if (timeout != std::chrono::nanoseconds::max()) {
      // We wait here until timeout or until something is pushed into the queue
      events_queue_cv_.wait_for(lock, timeout, has_event_predicate);
      if (event_queue_.empty()) {
        return false;
      } else {
        event = event_queue_.front();
        event_queue_.pop();
        return true;
      }
    } else {
      // We wait here until something has been pushed into the queue
      events_queue_cv_.wait(lock, has_event_predicate);
      event = event_queue_.front();
      event_queue_.pop();
      return true;
    }
  }

private:
  // The underlying queue implementation
  std::queue<rclcpp::executors::ExecutorEvent> event_queue_;
  // Mutex to protect the insertion/extraction of events in the queue
  mutable std::mutex push_mutex_;
  // Variable used to notify when an event is added to the queue
  std::condition_variable events_queue_cv_;

};

}  // namespace buffers
}  // namespace experimental
}  // namespace rclcpp


#endif  // RCLCPP__EXPERIMENTAL__BUFFERS__SIMPLE_EVENTS_QUEUE_HPP_
