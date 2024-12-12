
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/compute/util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

namespace arrow {
namespace compute {

TEST(TestDemean, BasicDemean) {
  constexpr int data_bufndx{1};
  const std::vector<int32_t> test_result{0, 0, 0, 0, 0, 0};
  std::vector<int32_t> test_values{1, 1, 1, 1, 1, 1};
  Int32Builder input_builder;
  ASSERT_OK(input_builder.Reserve(test_values.size()));
  ASSERT_OK(input_builder.AppendValues(test_values));
  ASSERT_OK_AND_ASSIGN(auto test_inputs, input_builder.Finish());

  ASSERT_OK_AND_ASSIGN(Datum demean_result, CallFunction("demean", {test_inputs}));
  auto result_data = *(demean_result.array());

  // validate each value
  for (int val_ndx = 0; val_ndx < test_inputs->length(); ++val_ndx) {
    int32_t expected_value = test_result[val_ndx];
    int32_t actual_value = result_data.GetValues<uint64_t>(data_bufndx)[val_ndx];
    ASSERT_EQ(expected_value, actual_value);
  }
}
}  // namespace compute
}  // namespace arrow
