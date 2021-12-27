/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sstream>
#include <string>
#include <vector>

namespace tensorpipe {

inline std::string joinStrs(const std::vector<std::string>& strs) {
  if (strs.empty()) {
    return "";
  }
  std::ostringstream oss;
  oss << strs[0];
  for (size_t idx = 1; idx < strs.size(); idx++) {
    oss << ", " << strs[idx];
  }
  return oss.str();
}

template <typename T>
std::string formatMatrix(const std::vector<std::vector<T>>& matrix) {
  std::ostringstream oss;
  oss << "{";
  for (size_t rowIdx = 0; rowIdx < matrix.size(); rowIdx++) {
    if (rowIdx > 0) {
      oss << ", ";
    }
    oss << "{";
    for (size_t colIdx = 0; colIdx < matrix[rowIdx].size(); colIdx++) {
      if (colIdx > 0) {
        oss << ", ";
      }
      oss << matrix[rowIdx][colIdx];
    }
    oss << "}";
  }
  oss << "}";
  return oss.str();
}

// Since text manipulation is hard, let's use this to double-check our results.
inline bool isValidUuid(const std::string& uuid) {
  // Check it's in this format:
  // aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
  // |0   |5   |10  |15  |20  |25  |30  |35
  if (uuid.size() != 36) {
    return false;
  }
  for (int i = 0; i < uuid.size(); i++) {
    if (i == 8 || i == 13 || i == 18 || i == 23) {
      if (uuid[i] != '-') {
        return false;
      }
    } else {
      if (!((uuid[i] >= '0' && uuid[i] <= '9') ||
            (uuid[i] >= 'a' && uuid[i] <= 'f'))) {
        return false;
      }
    }
  }
  return true;
}

} // namespace tensorpipe
