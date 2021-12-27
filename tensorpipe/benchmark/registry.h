/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// NB: This Registry works poorly when you have other namespaces.

/**
 * Simple registry implementation that uses static variables to
 * register object creators during program initialization time. This registry
 * implementation is largely borrowed from the PyTorch registry utility in file
 * pytorch/c10/util/Registry.h.
 */

#pragma once

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace tensorpipe {

/**
 * @brief A template class that allows one to register classes by keys.
 *
 * The keys are usually a std::string specifying the name, but can be anything
 * that can be used in a std::map.
 *
 * You should most likely not use the Registry class explicitly, but use the
 * helper macros below to declare specific registries as well as registering
 * objects.
 */
template <class ObjectPtrType, class... Args>
class Registry {
 public:
  typedef std::function<ObjectPtrType(Args...)> Creator;

  Registry() : registry_() {}

  // Adds a key and its associated creator to the desired registry. If the key
  // already exists in the registry, we simply replace the old creator
  // with the new args for the key.
  void registerCreator(std::string key, Creator creator) {
    registry_[key] = creator;
  }

  // Allows you to register and key/Creator pair and provide a help_messge for
  // the key as well.
  void registerCreator(
      std::string key,
      Creator creator,
      const std::string& helpMsg) {
    registerCreator(key, creator);
    helpMessage_[key] = helpMsg;
  }

  // Returns whether a particular key exists in the given registry.
  inline bool has(std::string key) {
    return (registry_.count(key) != 0);
  }

  // Given the key, create() invokes the creator with the provided args and
  // returns the object that the creator function constructs.
  ObjectPtrType create(std::string key, Args... args) {
    if (registry_.count(key) == 0) {
      // Returns nullptr if the key is not registered.
      return nullptr;
    }
    return registry_[key](args...);
  }

  // Returns the registered keys as a std::vector.
  std::vector<std::string> keys() const {
    std::vector<std::string> keys;
    for (const auto& it : registry_) {
      keys.push_back(it.first);
    }
    return keys;
  }

  // Returns the help_message for the key if one is provided.
  inline const std::unordered_map<std::string, std::string>& helpMessage()
      const {
    return helpMessage_;
  }

  const char* helpMessage(std::string key) const {
    auto it = helpMessage_.find(key);
    if (it == helpMessage_.end()) {
      return nullptr;
    }
    return it->second.c_str();
  }

 private:
  std::unordered_map<std::string, Creator> registry_;
  std::unordered_map<std::string, std::string> helpMessage_;
};

// Registerer is a class template that simplifies Register-ing keys for a given
// registry.
template <class ObjectPtrType, class... Args>
class Registerer {
 public:
  explicit Registerer(
      std::string key,
      Registry<ObjectPtrType, Args...>& registry,
      typename Registry<ObjectPtrType, Args...>::Creator creator,
      const std::string& helpMsg = "") {
    registry.registerCreator(key, creator, helpMsg);
  }
};

// The following macros should be used to create/add to registries. Avoid
// invoking the Registry class template functions directly.

#define TP_CONCATENATE_IMPL(s1, s2) s1##s2
#define TP_CONCATENATE(s1, s2) TP_CONCATENATE_IMPL(s1, s2)
#define TP_ANONYMOUS_VARIABLE(str) TP_CONCATENATE(str, __LINE__)

// Using the construct on first use idiom to avoid static order initialization
// issue. Refer to this link for reference:
// https://isocpp.org/wiki/faq/ctors#static-init-order-on-first-use
#define TP_DEFINE_TYPED_REGISTRY(RegistryName, ObjectType, PtrType, ...)     \
  tensorpipe::Registry<PtrType<ObjectType>, ##__VA_ARGS__>& RegistryName() { \
    static tensorpipe::Registry<PtrType<ObjectType>, ##__VA_ARGS__>*         \
        registry =                                                           \
            new tensorpipe::Registry<PtrType<ObjectType>, ##__VA_ARGS__>();  \
    return *registry;                                                        \
  }

#define TP_DECLARE_TYPED_REGISTRY(RegistryName, ObjectType, PtrType, ...)   \
  tensorpipe::Registry<PtrType<ObjectType>, ##__VA_ARGS__>& RegistryName(); \
  typedef tensorpipe::Registerer<PtrType<ObjectType>, ##__VA_ARGS__>        \
      Registerer##RegistryName

#define TP_DEFINE_SHARED_REGISTRY(RegistryName, ObjectType, ...) \
  TP_DEFINE_TYPED_REGISTRY(                                      \
      RegistryName, ObjectType, std::shared_ptr, ##__VA_ARGS__)

#define TP_DECLARE_SHARED_REGISTRY(RegistryName, ObjectType, ...) \
  TP_DECLARE_TYPED_REGISTRY(                                      \
      RegistryName, ObjectType, std::shared_ptr, ##__VA_ARGS__)

#define TP_REGISTER_TYPED_CREATOR(RegistryName, key, ...)                  \
  static Registerer##RegistryName TP_ANONYMOUS_VARIABLE(g_##RegistryName)( \
      key, RegistryName(), ##__VA_ARGS__);

#define TP_REGISTER_CREATOR(RegistryName, key, ...) \
  TP_REGISTER_TYPED_CREATOR(RegistryName, #key, __VA_ARGS__)

} // namespace tensorpipe
