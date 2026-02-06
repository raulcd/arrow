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

// Minimal sync Flight server for thread count comparison.

#include <dirent.h>
#include <chrono>
#include <iostream>
#include <thread>

#include "arrow/array/builder_primitive.h"
#include "arrow/flight/server.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"

int CountThreads() {
  int count = 0;
  auto* dir = opendir("/proc/self/task");
  if (dir) {
    while (readdir(dir)) count++;
    closedir(dir);
    return count - 2;
  }
  return -1;
}

// A slow stream that blocks for 30 seconds before sending each batch.
// This simulates a slow data source and keeps the server thread occupied.
class SlowStream : public arrow::flight::FlightDataStream {
 public:
  SlowStream() {
    schema_ = arrow::schema(
        {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())});
  }

  std::shared_ptr<arrow::Schema> schema() override { return schema_; }

  arrow::Result<arrow::flight::FlightPayload> GetSchemaPayload() override {
    arrow::flight::FlightPayload payload;
    auto ipc_options = arrow::ipc::IpcWriteOptions::Defaults();
    arrow::ipc::DictionaryFieldMapper mapper(*schema_);
    ARROW_RETURN_NOT_OK(arrow::ipc::GetSchemaPayload(*schema_, ipc_options, mapper,
                                                     &payload.ipc_message));
    return payload;
  }

  arrow::Result<arrow::flight::FlightPayload> Next() override {
    if (batches_sent_ >= 5) {
      return arrow::flight::FlightPayload{};  // End of stream
    }

    // Block for 5 seconds - this keeps the thread occupied
    std::this_thread::sleep_for(std::chrono::seconds(5));

    arrow::Int64Builder builder_a, builder_b;
    (void)builder_a.AppendValues({1, 2, 3, 4, 5});
    (void)builder_b.AppendValues({10, 20, 30, 40, 50});
    auto arr_a = *builder_a.Finish();
    auto arr_b = *builder_b.Finish();
    auto batch = arrow::RecordBatch::Make(schema_, 5, {arr_a, arr_b});

    arrow::flight::FlightPayload payload;
    auto ipc_options = arrow::ipc::IpcWriteOptions::Defaults();
    ARROW_RETURN_NOT_OK(
        arrow::ipc::GetRecordBatchPayload(*batch, ipc_options, &payload.ipc_message));
    batches_sent_++;
    return payload;
  }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  int batches_sent_ = 0;
};

class SyncFlightServer : public arrow::flight::FlightServerBase {
 public:
  arrow::Status DoGet(const arrow::flight::ServerCallContext&,
                      const arrow::flight::Ticket&,
                      std::unique_ptr<arrow::flight::FlightDataStream>* stream) override {
    *stream = std::make_unique<SlowStream>();
    return arrow::Status::OK();
  }
};

int main() {
  auto location = arrow::flight::Location::ForGrpcTcp("0.0.0.0", 31337).ValueOrDie();
  arrow::flight::FlightServerOptions options(location);

  SyncFlightServer server;
  auto status = server.Init(options);
  if (!status.ok()) {
    std::cerr << "Failed to init server: " << status.ToString() << std::endl;
    return 1;
  }

  std::cout << "Sync server listening on port 31337" << std::endl;

  std::thread counter([]() {
    while (true) {
      std::cout << "Threads: " << CountThreads() << std::endl;
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });
  counter.detach();

  status = server.Wait();
  if (!status.ok()) {
    std::cerr << "Server error: " << status.ToString() << std::endl;
    return 1;
  }
}
