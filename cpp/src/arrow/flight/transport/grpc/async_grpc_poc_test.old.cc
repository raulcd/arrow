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

/// \file async_grpc_poc_test.cc
/// \brief Tests for the async Flight DoGet/DoPut PoC server.
///
/// Validates:
/// 1. FlightDataSerialize/Deserialize work with ByteBuffer outside SerializationTraits
/// 2. Existing sync FlightClient can talk to the generic callback server
/// 3. Thread count scales with gRPC pool size, not stream count
/// 4. DoPut receives and processes streaming data correctly

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#ifdef __linux__
#  include <dirent.h>
#endif

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/flight/client.h"
#include "arrow/flight/transport/grpc/async_grpc_server.h"
#include "arrow/flight/types.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow::flight::transport::grpc {
namespace {

// ---------------------------------------------------------------------------
// Test data helpers
// ---------------------------------------------------------------------------

constexpr int kNumBatches = 5;
constexpr int kBatchSize = 1024;

std::shared_ptr<Schema> TestSchema() {
  return schema({field("a", int64()), field("b", int64())});
}

std::shared_ptr<RecordBatch> TestBatch() {
  // Build a batch with kBatchSize rows of simple data
  Int64Builder builder_a, builder_b;
  EXPECT_OK(builder_a.Reserve(kBatchSize));
  EXPECT_OK(builder_b.Reserve(kBatchSize));
  for (int i = 0; i < kBatchSize; ++i) {
    builder_a.UnsafeAppend(i);
    builder_b.UnsafeAppend(i * 10);
  }
  EXPECT_OK_AND_ASSIGN(auto arr_a, builder_a.Finish());
  EXPECT_OK_AND_ASSIGN(auto arr_b, builder_b.Finish());
  return RecordBatch::Make(TestSchema(), kBatchSize, {arr_a, arr_b});
}

/// Create a PayloadProducer that yields a schema + kNumBatches of test data.
arrow::Result<PayloadProducer> MakeTestProducer(const arrow::flight::Ticket&) {
  auto test_schema = TestSchema();
  auto mapper = std::make_shared<ipc::DictionaryFieldMapper>(*test_schema);
  auto ipc_options = ipc::IpcWriteOptions::Defaults();
  auto batch = TestBatch();

  // State shared across calls (captured by the lambda)
  auto sent_schema = std::make_shared<bool>(false);
  auto batches_sent = std::make_shared<int>(0);

  return [=]() -> arrow::Result<FlightPayload> {
    FlightPayload payload;

    if (!*sent_schema) {
      // First call: send schema
      RETURN_NOT_OK(ipc::GetSchemaPayload(*test_schema, ipc_options, *mapper,
                                          &payload.ipc_message));
      *sent_schema = true;
      return payload;
    }

    if (*batches_sent >= kNumBatches) {
      // Signal end of stream
      payload.ipc_message.metadata = nullptr;
      return payload;
    }

    // Send a RecordBatch
    RETURN_NOT_OK(ipc::GetRecordBatchPayload(*batch, ipc_options, &payload.ipc_message));
    (*batches_sent)++;
    return payload;
  };
}

// ---------------------------------------------------------------------------
// Thread counting utility (Linux only)
// ---------------------------------------------------------------------------

int CountThreads() {
#ifdef __linux__
  int count = 0;
  auto* dir = opendir("/proc/self/task");
  if (dir) {
    while (readdir(dir)) {
      count++;
    }
    closedir(dir);
    return count - 2;  // subtract . and ..
  }
#endif
  return -1;
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class AsyncFlightGrpcTest : public ::testing::Test {
 protected:
  void SetUp() override {
    service_.SetDoGetFactory(MakeTestProducer);

    ::grpc::ServerBuilder builder;
    builder.AddListeningPort("localhost:0", ::grpc::InsecureServerCredentials(), &port_);
    builder.SetMaxReceiveMessageSize(-1);
    builder.RegisterCallbackGenericService(&service_);

    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr) << "Failed to start async gRPC server";

    std::string uri = "grpc://localhost:" + std::to_string(port_);
    ASSERT_OK_AND_ASSIGN(auto location, arrow::flight::Location::Parse(uri));
    ASSERT_OK_AND_ASSIGN(client_, arrow::flight::FlightClient::Connect(location));
  }

  void TearDown() override {
    if (client_) {
      ASSERT_OK(client_->Close());
    }
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
  }

  FlightCallbackService service_;
  std::unique_ptr<::grpc::Server> server_;
  int port_ = 0;
  std::unique_ptr<arrow::flight::FlightClient> client_;
};

// ---------------------------------------------------------------------------
// Test 1: Basic DoGet -- sync client talks to async callback server
// ---------------------------------------------------------------------------

TEST_F(AsyncFlightGrpcTest, BasicDoGet) {
  arrow::flight::Ticket ticket{"test-ticket"};
  ASSERT_OK_AND_ASSIGN(auto reader, client_->DoGet(ticket));

  // Read and verify schema
  std::shared_ptr<Schema> result_schema;
  ASSERT_OK(reader->GetSchema().Value(&result_schema));

  auto expected_schema = TestSchema();
  ASSERT_TRUE(result_schema->Equals(*expected_schema))
      << "Schema mismatch.\nExpected: " << expected_schema->ToString()
      << "\nGot: " << result_schema->ToString();

  // Read and verify all batches
  auto expected_batch = TestBatch();
  int batch_count = 0;
  while (true) {
    ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
    if (!chunk.data) break;

    ASSERT_TRUE(chunk.data->Equals(*expected_batch))
        << "Batch " << batch_count << " mismatch.\n"
        << "Expected rows: " << expected_batch->num_rows()
        << " Got rows: " << chunk.data->num_rows();
    batch_count++;
  }

  ASSERT_EQ(batch_count, kNumBatches)
      << "Expected " << kNumBatches << " batches, got " << batch_count;
}

// ---------------------------------------------------------------------------
// Test 2: Multiple sequential DoGet calls
// ---------------------------------------------------------------------------

TEST_F(AsyncFlightGrpcTest, MultipleSequentialDoGet) {
  for (int i = 0; i < 5; ++i) {
    arrow::flight::Ticket ticket{"seq-test-" + std::to_string(i)};
    ASSERT_OK_AND_ASSIGN(auto reader, client_->DoGet(ticket));

    int batch_count = 0;
    while (true) {
      ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
      if (!chunk.data) break;
      batch_count++;
    }
    ASSERT_EQ(batch_count, kNumBatches);
  }
}

// ---------------------------------------------------------------------------
// Test 3: Concurrent DoGet streams -- measure thread count
// ---------------------------------------------------------------------------

TEST_F(AsyncFlightGrpcTest, ConcurrentStreamsThreadCount) {
  constexpr int kConcurrentStreams = 100;

  int threads_before = CountThreads();
  if (threads_before < 0) {
    GTEST_SKIP() << "Thread counting not available on this platform";
  }
  std::cout << "[Thread count] Before: " << threads_before << std::endl;

  // Open many concurrent DoGet streams from separate client threads.
  // The streams are opened but NOT drained yet, so the server must hold
  // all of them open simultaneously.
  std::vector<std::unique_ptr<arrow::flight::FlightStreamReader>> readers;
  std::mutex readers_mutex;
  std::atomic<int> started{0};
  std::atomic<int> errors{0};

  {
    std::vector<std::thread> launchers;
    for (int i = 0; i < kConcurrentStreams; ++i) {
      launchers.emplace_back([&, i]() {
        arrow::flight::Ticket ticket{"concurrent-" + std::to_string(i)};
        auto result = client_->DoGet(ticket);
        if (result.ok()) {
          std::lock_guard<std::mutex> lock(readers_mutex);
          readers.push_back(std::move(*result));
          started++;
        } else {
          errors++;
          std::cerr << "DoGet error: " << result.status().ToString() << std::endl;
        }
      });
    }
    for (auto& t : launchers) {
      t.join();
    }
  }

  std::cout << "[Streams] Started: " << started.load() << "  Errors: " << errors.load()
            << std::endl;

  // Let gRPC thread pool stabilize
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  int threads_during = CountThreads();
  std::cout << "[Thread count] During " << kConcurrentStreams
            << " concurrent streams: " << threads_during << std::endl;
  std::cout << "[Thread count] Increase: " << (threads_during - threads_before)
            << std::endl;

  // Drain all streams
  for (auto& reader : readers) {
    while (true) {
      auto chunk = reader->Next();
      if (!chunk.ok() || !chunk->data) break;
    }
  }
  readers.clear();

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  int threads_after = CountThreads();
  std::cout << "[Thread count] After draining: " << threads_after << std::endl;

  // Key assertion: with the async server the thread count should NOT scale
  // linearly with stream count. Allow generous headroom for gRPC pool threads
  // but expect far fewer than kConcurrentStreams new threads.
  EXPECT_LT(threads_during, threads_before + kConcurrentStreams / 2)
      << "Thread count scaled too much with stream count. "
      << "The async server may not be reducing threads as expected.";
}

// ---------------------------------------------------------------------------
// Test 4: Basic DoPut -- send data to async server
// ---------------------------------------------------------------------------

TEST_F(AsyncFlightGrpcTest, BasicDoPut) {
  // Create a consumer that counts batches received
  auto batch_count = std::make_shared<int>(0);
  auto schema_received = std::make_shared<bool>(false);

  auto consumer = [batch_count, schema_received](
                      arrow::flight::internal::FlightData data) -> arrow::Status {
    if (data.metadata == nullptr && data.body == nullptr) {
      // End marker
      return arrow::Status::OK();
    }

    if (data.metadata && !*schema_received) {
      // Schema message - just mark that we received it
      *schema_received = true;
      return arrow::Status::OK();
    }

    if (data.metadata && *schema_received) {
      // Record batch
      (*batch_count)++;
    }
    return arrow::Status::OK();
  };

  service_.SetDoPutConsumer(consumer);

  // Send data via client
  auto batch = TestBatch();
  auto schema = TestSchema();
  arrow::flight::FlightDescriptor descriptor{
      arrow::flight::FlightDescriptor::PATH, "", {"test-put"}};

  ASSERT_OK_AND_ASSIGN(auto result, client_->DoPut(descriptor, schema));
  auto& writer = result.writer;

  // Send schema implicitly via writer
  // Send a few batches
  for (int i = 0; i < kNumBatches; ++i) {
    ASSERT_OK(writer->WriteRecordBatch(*batch));
  }
  ASSERT_OK(writer->Close());

  // Give server time to process
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Verify batches were received (should be at least what we sent)
  EXPECT_GE(*batch_count, kNumBatches) << "Expected to receive at least " << kNumBatches
                                       << " batches, got " << *batch_count;
}

// ---------------------------------------------------------------------------
// Test 5: Multiple sequential DoPut calls
// ---------------------------------------------------------------------------

TEST_F(AsyncFlightGrpcTest, MultipleSequentialDoPut) {
  for (int put_num = 0; put_num < 5; ++put_num) {
    auto batch_count = std::make_shared<int>(0);

    auto consumer =
        [batch_count](arrow::flight::internal::FlightData data) -> arrow::Status {
      if (data.metadata && data.body) {
        (*batch_count)++;
      }
      return arrow::Status::OK();
    };

    service_.SetDoPutConsumer(consumer);

    auto batch = TestBatch();
    auto schema = TestSchema();
    arrow::flight::FlightDescriptor descriptor{arrow::flight::FlightDescriptor::PATH,
                                               "",
                                               {"seq-put-" + std::to_string(put_num)}};

    ASSERT_OK_AND_ASSIGN(auto result, client_->DoPut(descriptor, schema));
    auto& writer = result.writer;
    for (int i = 0; i < 3; ++i) {
      ASSERT_OK(writer->WriteRecordBatch(*batch));
    }
    ASSERT_OK(writer->Close());

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_GE(*batch_count, 3);
  }
}

// ---------------------------------------------------------------------------
// Test 6: Concurrent DoPut streams
// ---------------------------------------------------------------------------

TEST_F(AsyncFlightGrpcTest, ConcurrentDoPutStreams) {
  constexpr int kConcurrentPuts = 20;

  auto total_batches = std::make_shared<std::atomic<int>>(0);
  auto consumer =
      [total_batches](arrow::flight::internal::FlightData data) -> arrow::Status {
    if (data.metadata && data.body) {
      total_batches->fetch_add(1, std::memory_order_relaxed);
    }
    return arrow::Status::OK();
  };

  service_.SetDoPutConsumer(consumer);

  auto batch = TestBatch();
  auto schema = TestSchema();

  // Launch concurrent DoPut streams
  {
    std::vector<std::thread> launchers;
    for (int i = 0; i < kConcurrentPuts; ++i) {
      launchers.emplace_back([&, i]() {
        arrow::flight::FlightDescriptor descriptor{
            arrow::flight::FlightDescriptor::PATH,
            "",
            {"concurrent-put-" + std::to_string(i)}};
        auto result = client_->DoPut(descriptor, schema);
        if (result.ok()) {
          auto& writer = result.ValueOrDie().writer;
          for (int j = 0; j < 2; ++j) {
            EXPECT_OK(writer->WriteRecordBatch(*batch));
          }
          EXPECT_OK(writer->Close());
        }
      });
    }
    for (auto& t : launchers) {
      t.join();
    }
  }

  // Give server time to process all data
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Each stream sends 2 batches
  int expected = kConcurrentPuts * 2;
  EXPECT_GE(total_batches->load(), expected)
      << "Expected at least " << expected << " batches from " << kConcurrentPuts
      << " concurrent streams, got " << total_batches->load();
}

}  // namespace
}  // namespace arrow::flight::transport::grpc
