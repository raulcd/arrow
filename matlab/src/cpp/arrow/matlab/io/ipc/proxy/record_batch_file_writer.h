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

#include "arrow/ipc/writer.h"
#include "arrow/matlab/io/ipc/proxy/record_batch_writer.h"

#include "libmexclass/proxy/Proxy.h"

namespace arrow::matlab::io::ipc::proxy {

class RecordBatchFileWriter : public RecordBatchWriter {
 public:
  RecordBatchFileWriter(std::shared_ptr<arrow::ipc::RecordBatchWriter> writer);

  virtual ~RecordBatchFileWriter() = default;

  static libmexclass::proxy::MakeResult make(
      const libmexclass::proxy::FunctionArguments& constructor_arguments);
};

}  // namespace arrow::matlab::io::ipc::proxy
