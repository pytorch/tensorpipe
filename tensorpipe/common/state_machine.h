/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <deque>
#include <utility>

namespace tensorpipe {

template <typename TSubject, typename TOp>
class OpsStateMachine {
 public:
  class Iter {
   public:
    TOp& operator*() const {
      return *opPtr_;
    }

    TOp* operator->() const {
      return opPtr_;
    }

   private:
    explicit Iter(TOp* opPtr) : opPtr_(opPtr) {}

    TOp* opPtr_{nullptr};

    friend OpsStateMachine;
  };

  using Transitioner = void (TSubject::*)(Iter, typename TOp::State);

  OpsStateMachine(TSubject& subject, Transitioner transitioner)
      : subject_(subject), transitioner_(transitioner) {}

  template <typename... TArgs>
  Iter emplaceBack(uint64_t sequenceNumber, TArgs&&... args) {
    ops_.emplace_back(std::forward<TArgs>(args)...);
    TOp& op = ops_.back();
    op.sequenceNumber = sequenceNumber;
    return Iter(&op);
  }

  void advanceOperation(Iter initialOpIter) {
    // Advancing one operation may unblock later ones that could have progressed
    // but were prevented from overtaking. Thus each time an operation manages
    // to advance we'll try to also advance the one after.
    for (int64_t sequenceNumber = initialOpIter->sequenceNumber;;
         ++sequenceNumber) {
      TOp* opPtr = findOperation(sequenceNumber);
      if (opPtr == nullptr || opPtr->state == TOp::FINISHED ||
          !advanceOneOperation(*opPtr)) {
        break;
      }
    }
  }

  void advanceAllOperations() {
    // We cannot just iterate over the operations here as advanceOneOperation
    // could potentially erase some of them, thus invalidating references and/or
    // iterators.
    if (ops_.empty()) {
      return;
    }
    for (int64_t sequenceNumber = ops_.front().sequenceNumber;;
         ++sequenceNumber) {
      TOp* opPtr = findOperation(sequenceNumber);
      if (opPtr == nullptr) {
        break;
      }
      advanceOneOperation(*opPtr);
    }
  }

  void attemptTransition(
      Iter opIter,
      typename TOp::State from,
      typename TOp::State to,
      bool cond,
      std::initializer_list<void (TSubject::*)(Iter)> actions) {
    if (opIter->state == from && cond) {
      for (const auto& action : actions) {
        (subject_.*action)(opIter);
      }
      opIter->state = to;
    }
  }

 private:
  TOp* findOperation(int64_t sequenceNumber) {
    if (ops_.empty()) {
      return nullptr;
    }
    int64_t offset = sequenceNumber - ops_.front().sequenceNumber;
    if (offset < 0 || offset >= ops_.size()) {
      return nullptr;
    }
    TOp& op = ops_[offset];
    TP_DCHECK_EQ(op.sequenceNumber, sequenceNumber);
    return &op;
  }

  bool advanceOneOperation(TOp& op) {
    // Due to the check in attemptTransition, each time that an operation
    // advances its state we must check whether this unblocks some later
    // operations that could progress but weren't allowed to overtake. In order
    // to detect whether this operation is advancing we store its state at the
    // beginning and then compare it with the state at the end.
    typename TOp::State initialState = op.state;

    // The operations must advance in order: later operations cannot "overtake"
    // earlier ones. Thus if this operation would reach a more advanced state
    // than previous operation we won't perform the transition.
    TOp* prevOpPtr = findOperation(op.sequenceNumber - 1);
    typename TOp::State prevOpState =
        prevOpPtr != nullptr ? prevOpPtr->state : TOp::FINISHED;

    (subject_.*transitioner_)(Iter(&op), prevOpState);

    // Compute return value now in case we next delete the operation.
    bool hasAdvanced = op.state != initialState;

    if (op.state == TOp::FINISHED) {
      // We can't remove the op if it's "in the middle". And, therefore, once we
      // remove the op at the front, we must check if other ops now also get
      // "unblocked". In other words, we always remove as much as we can from
      // the front.
      while (!ops_.empty() && ops_.front().state == TOp::FINISHED) {
        ops_.pop_front();
      }
    }

    return hasAdvanced;
  }

  TSubject& subject_;
  const Transitioner transitioner_;
  std::deque<TOp> ops_;
};

} // namespace tensorpipe
