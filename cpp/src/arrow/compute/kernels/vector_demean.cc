
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

// Include necessary Arrow headers
#include "arrow/compute/function.h"
#include "arrow/compute/registry.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {
// Function documentation
const FunctionDoc demean_doc{
    "Perform a demean operation over all the elements of the array",
    ("Returns an array where every element has been removed from \n"
     "the mean of the array."),
    {"array"}};

class DemeanMetaFunction : public MetaFunction {
 public:
  DemeanMetaFunction() : MetaFunction("demean", Arity::Unary(), demean_doc) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    switch (args[0].kind()) {
      case Datum::ARRAY:
      case Datum::CHUNKED_ARRAY: {
        return Demean(*args[0].make_array(), ctx);
      } break;
      default:
        break;
    }
    return Status::NotImplemented(
        "Unsupported types for demean operation: "
        "values=",
        args[0].ToString());
  }

 private:
  template <typename T>
  static Result<Datum> Demean(const T& input, ExecContext* ctx) {
    arrow::Datum demean_datum;
    Datum mean_datum;
    ARROW_ASSIGN_OR_RAISE(mean_datum, arrow::compute::CallFunction("mean", {input}));
    ARROW_ASSIGN_OR_RAISE(demean_datum,
                          arrow::compute::CallFunction("subtract", {input, mean_datum}));
    return demean_datum;
  }
};
}  // namespace

void RegisterVectorDemean(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<DemeanMetaFunction>()));
}
}  // namespace internal
}  // namespace compute
}  // namespace arrow
