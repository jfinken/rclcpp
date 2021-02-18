// Copyright 2020 Open Source Robotics Foundation, Inc.
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

#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "rclcpp/exceptions/exceptions.hpp"
#include "rclcpp/executors/events_executor.hpp"

using namespace std::chrono_literals;

using rclcpp::executors::EventsExecutor;

EventsExecutor::EventsExecutor(
  rclcpp::experimental::buffers::EventsQueue::UniquePtr events_queue,
  const rclcpp::ExecutorOptions & options)
: rclcpp::Executor(options)
{
  timers_manager_ = std::make_shared<TimersManager>(context_);
  entities_collector_ = std::make_shared<EventsExecutorEntitiesCollector>(this);
  entities_collector_->init();


  // Setup the executor notifier to wake up the executor when some guard conditions are tiggered.
  // The added guard conditions are guaranteed to not go out of scope before the executor itself.
  executor_notifier_ = std::make_shared<EventsExecutorNotifyWaitable>();
  executor_notifier_->add_guard_condition(&shutdown_guard_condition_->get_rcl_guard_condition());
  executor_notifier_->add_guard_condition(&interrupt_guard_condition_);
  executor_notifier_->set_events_executor_callback(this, &EventsExecutor::push_event);
  entities_collector_->add_waitable(executor_notifier_);

  // Get ownership of the queue used to store events.
  events_queue_ = std::move(events_queue);
  // Init the events queue
  events_queue_->init();
}

void
EventsExecutor::spin()
{
  if (spinning.exchange(true)) {
    throw std::runtime_error("spin() called while already spinning");
  }
  RCLCPP_SCOPE_EXIT(this->spinning.store(false););

  // When condition variable is notified, check this predicate to proceed
  auto has_event_predicate = [this]() {return !events_queue_->empty();};

  timers_manager_->start();

  while (rclcpp::ok(context_) && spinning.load()) {
    std::unique_lock<std::mutex> push_lock(push_mutex_);
    // We wait here until something has been pushed to the event queue
    events_queue_cv_.wait(push_lock, has_event_predicate);
    // Local event queue to allow entities to push events while we execute them
    EventQueue execution_event_queue = events_queue_->get_all_events();
    // Unlock the mutex
    push_lock.unlock();
    // Consume all available events, this queue will be empty at the end of the function
    this->consume_all_events(execution_event_queue);
  }
  timers_manager_->stop();
}

void
EventsExecutor::spin_some(std::chrono::nanoseconds max_duration)
{
  if (spinning.exchange(true)) {
    throw std::runtime_error("spin_some() called while already spinning");
  }
  RCLCPP_SCOPE_EXIT(this->spinning.store(false););

  // In this context a 0 input max_duration means no duration limit
  if (std::chrono::nanoseconds(0) == max_duration) {
    max_duration = timers_manager_->MAX_TIME;
  }

  auto start = std::chrono::steady_clock::now();

  auto max_duration_not_elapsed = [max_duration, start]() {
      auto elapsed_time = std::chrono::steady_clock::now() - start;
      return elapsed_time < max_duration;
    };

  // Get the number of events ready at this time point
  std::unique_lock<std::mutex> lock(push_mutex_);
  size_t available_events_at_tp = events_queue_->size();
  lock.unlock();

  size_t executed_events = 0;

  // Checks if all events that were ready when spin_some was called, were executed.
  auto executed_ready_events_at_tp = [executed_events, available_events_at_tp]() {
      return executed_events >= available_events_at_tp;
    };

  // Execute events and timers ready when spin_some was called,
  // until timeout or no more work available.
  while (rclcpp::ok(context_) && spinning.load() && max_duration_not_elapsed()) {
    // Execute first event from queue if it exists
    if (!executed_ready_events_at_tp()) {
      std::unique_lock<std::mutex> lock(push_mutex_);

      bool has_event = !events_queue_->empty();

      if (has_event) {
        rmw_listener_event_t event = events_queue_->front();
        events_queue_->pop();
        // std::cout << "Execute event" << std::endl;
        this->execute_event(event);
        executed_events++;
        continue;
      }
    }

    // Execute timer, if was ready at start
    if (timers_manager_->execute_head_timer_if_ready_at_tp(start)) {
      continue;
    }

    // If there's no more work available, exit
    break;
  }
}

void
EventsExecutor::spin_all(std::chrono::nanoseconds max_duration)
{
  if (spinning.exchange(true)) {
    throw std::runtime_error("spin_all() called while already spinning");
  }

  if (max_duration < 0ns) {
    throw std::invalid_argument("max_duration must be positive");
  }

  RCLCPP_SCOPE_EXIT(this->spinning.store(false););

  // In this context a 0 input max_duration means no duration limit
  if (std::chrono::nanoseconds(0) == max_duration) {
    max_duration = timers_manager_->MAX_TIME;
  }

  auto start = std::chrono::steady_clock::now();

  auto max_duration_not_elapsed = [max_duration, start]() {
      auto elapsed_time = std::chrono::steady_clock::now() - start;
      return elapsed_time < max_duration;
    };

  // Execute timer and events until timeout or no more work available
  while (rclcpp::ok(context_) && spinning.load() && max_duration_not_elapsed()) {
    // Execute first event from queue if it exists
    {
      std::unique_lock<std::mutex> lock(push_mutex_);

      bool has_event = !events_queue_->empty();

      if (has_event) {
        rmw_listener_event_t event = events_queue_->front();
        events_queue_->pop();
        this->execute_event(event);
        continue;
      }
    }

    // Execute timer, if was ready
    if (timers_manager_->execute_head_timer()) {
      continue;
    }

    // If there's no more work available, exit
    break;
  }
}

void
EventsExecutor::spin_once_impl(std::chrono::nanoseconds timeout)
{
  // In this context a negative input timeout means no timeout
  if (timeout < 0ns) {
    timeout = timers_manager_->MAX_TIME;
  }

  // Select the smallest between input timeout and timer timeout
  auto next_timer_timeout = timers_manager_->get_head_timeout();
  if (next_timer_timeout < timeout) {
    timeout = next_timer_timeout;
  }

  // When condition variable is notified, check this predicate to proceed
  auto has_event_predicate = [this]() {return !events_queue_->empty();};

  rmw_listener_event_t event;
  bool has_event = false;

  {
    // Wait until timeout or event arrives
    std::unique_lock<std::mutex> lock(push_mutex_);
    events_queue_cv_.wait_for(lock, timeout, has_event_predicate);

    // Grab first event from queue if it exists
    has_event = !events_queue_->empty();
    if (has_event) {
      event = events_queue_->front();
      events_queue_->pop();
    }
  }

  // If we wake up from the wait with an event, it means that it
  // arrived before any of the timers expired.
  if (has_event) {
    this->execute_event(event);
  } else {
    timers_manager_->execute_head_timer();
  }
}

void
EventsExecutor::add_node(
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr, bool notify)
{
  // This field is unused because we don't have to wake up the executor when a node is added.
  (void) notify;

  // Add node to entities collector
  entities_collector_->add_node(node_ptr);
}

void
EventsExecutor::add_node(std::shared_ptr<rclcpp::Node> node_ptr, bool notify)
{
  this->add_node(node_ptr->get_node_base_interface(), notify);
}

void
EventsExecutor::remove_node(
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr, bool notify)
{
  // This field is unused because we don't have to wake up the executor when a node is removed.
  (void)notify;

  // Remove node from entities collector.
  // This will result in un-setting all the event callbacks from its entities.
  // After this function returns, this executor will not receive any more events associated
  // to these entities.
  entities_collector_->remove_node(node_ptr);
}

void
EventsExecutor::remove_node(std::shared_ptr<rclcpp::Node> node_ptr, bool notify)
{
  this->remove_node(node_ptr->get_node_base_interface(), notify);
}

void
EventsExecutor::consume_all_events(EventQueue & event_queue)
{
  while (!event_queue.empty()) {
    rmw_listener_event_t event = event_queue.front();
    event_queue.pop();

    this->execute_event(event);
  }
}

void
EventsExecutor::execute_event(const rmw_listener_event_t & event)
{
  switch (event.type) {
    case SUBSCRIPTION_EVENT:
      {
        auto subscription = entities_collector_->get_subscription(event.entity);

        if (subscription) {
          execute_subscription(subscription);
        }
        break;
      }

    case SERVICE_EVENT:
      {
        auto service = entities_collector_->get_service(event.entity);

        if (service) {
          execute_service(service);
        }
        break;
      }

    case CLIENT_EVENT:
      {
        auto client = entities_collector_->get_client(event.entity);

        if (client) {
          execute_client(client);
        }
        break;
      }

    case WAITABLE_EVENT:
      {
        auto waitable = entities_collector_->get_waitable(event.entity);

        if (waitable) {
          auto data = waitable->take_data();
          waitable->execute(data);
        }
        break;
      }
  }
}

void
EventsExecutor::add_callback_group(
  rclcpp::CallbackGroup::SharedPtr group_ptr,
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr,
  bool notify)
{
  // This field is unused because we don't have to wake up
  // the executor when a callback group is added.
  (void)notify;
  entities_collector_->add_callback_group(group_ptr, node_ptr);
}

void
EventsExecutor::remove_callback_group(
  rclcpp::CallbackGroup::SharedPtr group_ptr, bool notify)
{
  // This field is unused because we don't have to wake up
  // the executor when a callback group is removed.
  (void)notify;
  entities_collector_->remove_callback_group(group_ptr);
}

std::vector<rclcpp::CallbackGroup::WeakPtr>
EventsExecutor::get_all_callback_groups()
{
  return entities_collector_->get_all_callback_groups();
}

std::vector<rclcpp::CallbackGroup::WeakPtr>
EventsExecutor::get_manually_added_callback_groups()
{
  return entities_collector_->get_manually_added_callback_groups();
}

std::vector<rclcpp::CallbackGroup::WeakPtr>
EventsExecutor::get_automatically_added_callback_groups_from_nodes()
{
  return entities_collector_->get_automatically_added_callback_groups_from_nodes();
}
