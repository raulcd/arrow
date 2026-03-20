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

// Minimal reproducer to investigate whether grpc::ResourceQuota::SetMaxThreads
// can be used to limit the number of threads in a Flight server.

#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/flight/client.h"
#include "arrow/flight/server.h"
#include "arrow/flight/types.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"

namespace arrow::flight {

// Read thread count from /proc/self/status (Linux only).
int GetThreadCount() {
#ifdef __linux__
  std::ifstream status("/proc/self/status");
  std::string line;
  while (std::getline(status, line)) {
    if (line.compare(0, 8, "Threads:") == 0) {
      return std::stoi(line.substr(8));
    }
  }
#endif
  return -1;
}

// A Flight server whose DoGet holds streams open to keep threads alive.
class SlowFlightServer : public FlightServerBase {
 public:
  Status DoGet(const ServerCallContext&, const Ticket&,
               std::unique_ptr<FlightDataStream>* data_stream) override {
    // Build a small batch
    auto schema = arrow::schema({arrow::field("a", arrow::int64())});
    Int64Builder builder;
    RETURN_NOT_OK(builder.AppendValues({1, 2, 3}));
    ARROW_ASSIGN_OR_RAISE(auto arr, builder.Finish());
    auto batch = RecordBatch::Make(schema, 3, {arr});

    // Send one batch then sleep to hold the thread
    auto reader = RecordBatchReader::Make({batch}).ValueOrDie();
    *data_stream = std::make_unique<RecordBatchStream>(std::move(reader));

    // Sleep here to hold the gRPC thread alive for the duration
    std::this_thread::sleep_for(std::chrono::seconds(5));
    return Status::OK();
  }
};

struct ThreadTestResult {
  int threads_before;
  int threads_during;
  int streams_opened;
  int streams_failed;
};

// Start a server, open many concurrent DoGet streams, measure thread count.
ThreadTestResult RunThreadTest(int num_concurrent_streams,
                               std::function<void(void*)> builder_hook = nullptr) {
  SlowFlightServer server;
  EXPECT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("localhost", 0));

  FlightServerOptions options(location);
  if (builder_hook) {
    options.builder_hook = std::move(builder_hook);
  }
  EXPECT_OK(server.Init(options));

  int port = server.port();
  std::string uri = "grpc://localhost:" + std::to_string(port);

  ThreadTestResult result{};
  result.threads_before = GetThreadCount();

  // Open many concurrent DoGet streams
  std::vector<std::thread> client_threads;
  std::atomic<int> streams_opened{0};
  std::atomic<int> streams_failed{0};

  std::mutex error_mutex;
  std::vector<std::string> error_messages;

  for (int i = 0; i < num_concurrent_streams; i++) {
    client_threads.emplace_back([&uri, &streams_opened, &streams_failed, &error_mutex,
                                 &error_messages, i]() {
      ASSERT_OK_AND_ASSIGN(auto loc, Location::Parse(uri));
      ASSERT_OK_AND_ASSIGN(auto client, FlightClient::Connect(loc));
      Ticket ticket{"test"};
      auto reader_result = client->DoGet(ticket);
      if (!reader_result.ok()) {
        streams_failed++;
        std::lock_guard<std::mutex> lock(error_mutex);
        error_messages.push_back("Stream " + std::to_string(i) +
                                 " DoGet failed: " + reader_result.status().ToString());
        return;
      }
      streams_opened++;
      // Read the data (server will sleep during DoGet, holding the thread)
      auto reader = std::move(reader_result).ValueUnsafe();
      while (true) {
        auto chunk_result = reader->Next();
        if (!chunk_result.ok()) {
          std::lock_guard<std::mutex> lock(error_mutex);
          error_messages.push_back("Stream " + std::to_string(i) +
                                   " Next() failed: " + chunk_result.status().ToString());
          break;
        }
        if (!chunk_result->data) break;
      }
      (void)client->Close();
    });
  }

  // Wait a moment for streams to be established then measure threads
  std::this_thread::sleep_for(std::chrono::seconds(2));
  result.threads_during = GetThreadCount();

  // Wait for all client threads to finish
  for (auto& t : client_threads) {
    t.join();
  }

  result.streams_opened = streams_opened.load();
  result.streams_failed = streams_failed.load();

  // Print collected error messages
  if (!error_messages.empty()) {
    std::cout << "Error messages (" << error_messages.size() << " total):" << std::endl;
    // Print first few unique errors to avoid flooding output
    std::set<std::string> unique_errors;
    for (const auto& msg : error_messages) {
      unique_errors.insert(msg.substr(msg.find("failed:")));
    }
    for (const auto& err : unique_errors) {
      std::cout << "  " << err << std::endl;
    }
  }

  EXPECT_OK(server.Shutdown());
  return result;
}

TEST(GrpcResourceQuota, ThreadCountWithoutLimit) {
  const int kNumStreams = 100;
  auto result = RunThreadTest(kNumStreams);

  std::cout << "=== WITHOUT thread limit ===" << std::endl;
  std::cout << "Threads before: " << result.threads_before << std::endl;
  std::cout << "Threads during " << kNumStreams
            << " concurrent streams: " << result.threads_during << std::endl;
  std::cout << "Streams opened: " << result.streams_opened << std::endl;
  std::cout << "Streams failed: " << result.streams_failed << std::endl;

  // Without a limit, we expect many threads to be created
  ASSERT_GT(result.threads_during, result.threads_before);
}

TEST(GrpcResourceQuota, ThreadCountWithLimit) {
  const int kNumStreams = 100;
  const int kMaxThreads = 20;

  auto builder_hook = [](void* raw_builder) {
    auto* builder = reinterpret_cast<::grpc::ServerBuilder*>(raw_builder);
    ::grpc::ResourceQuota rq("flight_thread_test");
    rq.SetMaxThreads(kMaxThreads);
    builder->SetResourceQuota(rq);
  };

  auto result = RunThreadTest(kNumStreams, builder_hook);

  std::cout << "=== WITH SetMaxThreads(" << kMaxThreads << ") ===" << std::endl;
  std::cout << "Threads before: " << result.threads_before << std::endl;
  std::cout << "Threads during " << kNumStreams
            << " concurrent streams: " << result.threads_during << std::endl;
  std::cout << "Streams opened: " << result.streams_opened << std::endl;
  std::cout << "Streams failed: " << result.streams_failed << std::endl;

  // With a limit, thread count should be capped
  // Note: the total process threads include non-gRPC threads, so we check
  // that the delta is bounded
  int thread_delta = result.threads_during - result.threads_before;
  std::cout << "Thread delta: " << thread_delta << std::endl;
  std::cout << "Expected max delta ~" << kMaxThreads << std::endl;
}

TEST(GrpcResourceQuota, MaxPollers) {
  const int kNumStreams = 100;
  const int kMaxPollers = 20;

  auto builder_hook = [kMaxPollers](void* raw_builder) {
    auto* builder = reinterpret_cast<::grpc::ServerBuilder*>(raw_builder);
    builder->SetSyncServerOption(::grpc::ServerBuilder::SyncServerOption::MAX_POLLERS,
                                 kMaxPollers);
  };

  auto result = RunThreadTest(kNumStreams, builder_hook);

  std::cout << "=== WITH MAX_POLLERS(" << kMaxPollers << ") ===" << std::endl;
  std::cout << "Threads before: " << result.threads_before << std::endl;
  std::cout << "Threads during " << kNumStreams
            << " concurrent streams: " << result.threads_during << std::endl;
  std::cout << "Streams opened: " << result.streams_opened << std::endl;
  std::cout << "Streams failed: " << result.streams_failed << std::endl;
  int thread_delta = result.threads_during - result.threads_before;
  std::cout << "Thread delta: " << thread_delta << std::endl;
}

TEST(GrpcResourceQuota, MaxConcurrentStreams) {
  const int kNumStreams = 100;
  const int kMaxConcurrentStreams = 20;

  auto builder_hook = [kMaxConcurrentStreams](void* raw_builder) {
    auto* builder = reinterpret_cast<::grpc::ServerBuilder*>(raw_builder);
    builder->AddChannelArgument(GRPC_ARG_MAX_CONCURRENT_STREAMS, kMaxConcurrentStreams);
  };

  auto result = RunThreadTest(kNumStreams, builder_hook);

  std::cout << "=== WITH GRPC_ARG_MAX_CONCURRENT_STREAMS(" << kMaxConcurrentStreams
            << ") ===" << std::endl;
  std::cout << "Threads before: " << result.threads_before << std::endl;
  std::cout << "Threads during " << kNumStreams
            << " concurrent streams: " << result.threads_during << std::endl;
  std::cout << "Streams opened: " << result.streams_opened << std::endl;
  std::cout << "Streams failed: " << result.streams_failed << std::endl;
  int thread_delta = result.threads_during - result.threads_before;
  std::cout << "Thread delta: " << thread_delta << std::endl;
}

}  // namespace arrow::flight
