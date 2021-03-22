// Copyright 2019 Nobleo Technology
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

#ifndef RCLCPP__EXECUTORS__STATIC_SINGLE_THREADED_EXECUTOR_HPP_
#define RCLCPP__EXECUTORS__STATIC_SINGLE_THREADED_EXECUTOR_HPP_

#include <chrono>
#include <cassert>
#include <cstdlib>
#include <memory>
#include <vector>
#include <string>

#include "rmw/rmw.h"

#include "rclcpp/executor.hpp"
#include "rclcpp/executors/static_executor_entities_collector.hpp"
#include "rclcpp/experimental/executable_list.hpp"
#include "rclcpp/macros.hpp"
#include "rclcpp/memory_strategies.hpp"
#include "rclcpp/node.hpp"
#include "rclcpp/rate.hpp"
#include "rclcpp/utilities.hpp"
#include "rclcpp/visibility_control.hpp"

namespace rclcpp
{
namespace executors
{

/// Static executor implementation
/**
 * This executor is a static version of the original single threaded executor.
 * It's static because it doesn't reconstruct the executable list for every iteration.
 * All nodes, callbackgroups, timers, subscriptions etc. are created before
 * spin() is called, and modified only when an entity is added/removed to/from a node.
 *
 * To run this executor instead of SingleThreadedExecutor replace:
 * rclcpp::executors::SingleThreadedExecutor exec;
 * by
 * rclcpp::executors::StaticSingleThreadedExecutor exec;
 * in your source code and spin node(s) in the following way:
 * exec.add_node(node);
 * exec.spin();
 * exec.remove_node(node);
 */
class StaticSingleThreadedExecutor : public rclcpp::Executor
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(StaticSingleThreadedExecutor)

  /// Default constructor. See the default constructor for Executor.
  RCLCPP_PUBLIC
  explicit StaticSingleThreadedExecutor(
    const rclcpp::ExecutorOptions & options = rclcpp::ExecutorOptions());

  /// Default destrcutor.
  RCLCPP_PUBLIC
  virtual ~StaticSingleThreadedExecutor();

  /// Static executor implementation of spin.
  /**
   * This function will block until work comes in, execute it, and keep blocking.
   * It will only be interrupted by a CTRL-C (managed by the global signal handler).
   * \throws std::runtime_error when spin() called while already spinning
   */
  RCLCPP_PUBLIC
  void
  spin() override;

  /// Add a node to the executor.
  /**
   * An executor can have zero or more nodes which provide work during `spin` functions.
   * \param[in] node_ptr Shared pointer to the node to be added.
   * \param[in] notify True to trigger the interrupt guard condition during this function. If
   * the executor is blocked at the rmw layer while waiting for work and it is notified that a new
   * node was added, it will wake up.
   * \throw std::runtime_error if node was already added or if rcl_trigger_guard_condition
   * return an error
   */
  RCLCPP_PUBLIC
  void
  add_node(
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr,
    bool notify = true) override;

  /// Convenience function which takes Node and forwards NodeBaseInterface.
  /**
   * \throw std::runtime_error if node was already added or if rcl_trigger_guard_condition
   * returns an error
   */
  RCLCPP_PUBLIC
  void
  add_node(std::shared_ptr<rclcpp::Node> node_ptr, bool notify = true) override;

  /// Remove a node from the executor.
  /**
   * \param[in] node_ptr Shared pointer to the node to remove.
   * \param[in] notify True to trigger the interrupt guard condition and wake up the executor.
   * This is useful if the last node was removed from the executor while the executor was blocked
   * waiting for work in another thread, because otherwise the executor would never be notified.
   * \throw std::runtime_error if rcl_trigger_guard_condition returns an error
   */
  RCLCPP_PUBLIC
  void
  remove_node(
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr,
    bool notify = true) override;

  /// Convenience function which takes Node and forwards NodeBaseInterface.
  /**
   * \throw std::runtime_error if rcl_trigger_guard_condition returns an error
   */
  RCLCPP_PUBLIC
  void
  remove_node(std::shared_ptr<rclcpp::Node> node_ptr, bool notify = true) override;

  /// Static executor implementation of spin some
  /**
   * This non-blocking function will execute entities that
   * were ready when this API was called, until timeout or no
   * more work available. Entities that got ready while
   * executing work, won't be taken into account here.
   *
   * Example:
   *   while(condition) {
   *     spin_some();
   *     sleep(); // User should have some sync work or
   *              // sleep to avoid a 100% CPU usage
   *   }
   */
  RCLCPP_PUBLIC
  void
  spin_some(std::chrono::nanoseconds max_duration = std::chrono::nanoseconds(0)) override;

  /// Static executor implementation of spin all
  /**
   * This non-blocking function will execute entities until
   * timeout or no more work available. If new entities get ready
   * while executing work available, they will be executed
   * as long as the timeout hasn't expired.
   *
   * Example:
   *   while(condition) {
   *     spin_all();
   *     sleep(); // User should have some sync work or
   *              // sleep to avoid a 100% CPU usage
   *   }
   */
  RCLCPP_PUBLIC
  void
  spin_all(std::chrono::nanoseconds max_duration);

protected:
  /**
   * @brief Executes ready executables from wait set.
   * @param spin_once if true executes only the first ready executable.
   * @return true if any executable was ready.
   */
  RCLCPP_PUBLIC
  bool
  execute_ready_executables(bool spin_once = false);

  RCLCPP_PUBLIC
  void
  spin_some_impl(std::chrono::nanoseconds max_duration, bool exhaustive);

  RCLCPP_PUBLIC
  void
  spin_once_impl(std::chrono::nanoseconds timeout) override;

private:
  RCLCPP_DISABLE_COPY(StaticSingleThreadedExecutor)

  StaticExecutorEntitiesCollector::SharedPtr entities_collector_;
};

}  // namespace executors
}  // namespace rclcpp

#endif  // RCLCPP__EXECUTORS__STATIC_SINGLE_THREADED_EXECUTOR_HPP_
