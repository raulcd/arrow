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

// Minimal test to demonstrate Flight server thread-per-request behavior.
//
// This test creates a Flight server and spawns many concurrent DoExchange
// calls to show that each call consumes a thread.
//
// Build (from cpp/build directory):
//   cmake .. -DARROW_FLIGHT=ON -DARROW_BUILD_EXAMPLES=ON
//   make flight_thread_test
//
// Run:
//   ./release/flight_thread_test --num_clients=100

#include <sys/types.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "arrow/array/builder_primitive.h"
#include "arrow/flight/api.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace flight = arrow::flight;

// Global counters to track concurrent handlers
std::atomic<int> g_active_handlers{0};
std::atomic<int> g_peak_handlers{0};

// Get thread count for current process (Linux-specific)
int GetThreadCount() {
#ifdef __linux__
  std::ifstream stat_file("/proc/self/stat");
  std::string line;
  std::getline(stat_file, line);

  // Thread count is field 20 (0-indexed: 19) in /proc/self/stat
  // Easier approach: count entries in /proc/self/task/
  int count = 0;
  std::ifstream task_count("/proc/self/status");
  std::string field;
  while (task_count >> field) {
    if (field == "Threads:") {
      task_count >> count;
      break;
    }
  }
  return count;
#else
  return -1;  // Not implemented for non-Linux
#endif
}

// Simple Flight server that holds DoExchange calls open
class ThreadTestServer : public flight::FlightServerBase {
 public:
  explicit ThreadTestServer(int hold_seconds) : hold_seconds_(hold_seconds) {}

  arrow::Status DoExchange(const flight::ServerCallContext& context,
                           std::unique_ptr<flight::FlightMessageReader> reader,
                           std::unique_ptr<flight::FlightMessageWriter> writer) override {
    // Track concurrent handlers
    int current = ++g_active_handlers;
    int peak = g_peak_handlers.load();
    while (current > peak && !g_peak_handlers.compare_exchange_weak(peak, current)) {
      // Update peak if needed
    }

    // Hold the connection open (simulating work or waiting)
    std::this_thread::sleep_for(std::chrono::seconds(hold_seconds_));

    --g_active_handlers;

    // Send minimal response
    ARROW_RETURN_NOT_OK(writer->WriteMetadata(arrow::Buffer::FromString("done")));
    return arrow::Status::OK();
  }

 private:
  int hold_seconds_;
};

// Client function that makes a DoExchange call
arrow::Status MakeExchangeCall(flight::FlightClient* client) {
  auto descriptor = flight::FlightDescriptor::Command("test");
  ARROW_ASSIGN_OR_RAISE(auto exchange, client->DoExchange(descriptor));

  // Read response
  ARROW_ASSIGN_OR_RAISE(auto chunk, exchange.reader->Next());
  ARROW_RETURN_NOT_OK(exchange.writer->Close());

  return arrow::Status::OK();
}

void PrintUsage() {
  std::cout << "Usage: flight_thread_test [options]\n"
            << "  --num_clients=N    Number of concurrent clients (default: 100)\n"
            << "  --hold_seconds=N   How long each handler holds (default: 5)\n"
            << "  --port=N           Server port (default: 0 for auto)\n"
            << "  --max_threads=N    Limit gRPC thread pool (default: 0 = unlimited)\n";
}

int main(int argc, char** argv) {
  std::cout << "The PID: " << getpid() << "\n";
  int num_clients = 100;
  int hold_seconds = 30;
  int port = 0;
  int max_threads = 0;  // 0 = unlimited
  // Simple argument parsing
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.find("--num_clients=") == 0) {
      num_clients = std::stoi(arg.substr(14));
    } else if (arg.find("--hold_seconds=") == 0) {
      hold_seconds = std::stoi(arg.substr(15));
    } else if (arg.find("--port=") == 0) {
      port = std::stoi(arg.substr(7));
    } else if (arg.find("--max_threads=") == 0) {
      max_threads = std::stoi(arg.substr(14));
    } else if (arg == "--help" || arg == "-h") {
      PrintUsage();
      return 0;
    }
  }

  std::cout << "=== Flight Server Thread Test ===\n"
            << "Concurrent clients: " << num_clients << "\n"
            << "Hold time per request: " << hold_seconds << " seconds\n"
            << "Max threads: "
            << (max_threads > 0 ? std::to_string(max_threads) : "unlimited") << "\n\n";

  // Record baseline thread count
  int baseline_threads = GetThreadCount();
  std::cout << "Baseline thread count: " << baseline_threads << "\n\n";

  // Start server
  auto server = std::make_unique<ThreadTestServer>(hold_seconds);

  auto location_result = flight::Location::ForGrpcTcp("127.0.0.1", port);
  if (!location_result.ok()) {
    std::cerr << "Failed to create location: " << location_result.status() << "\n";
    return 1;
  }

  flight::FlightServerOptions options(*location_result);

  // If max_threads specified, use builder_hook to limit gRPC thread pool
  if (max_threads > 0) {
    options.builder_hook = [max_threads](void* builder) {
      auto* grpc_builder = static_cast<::grpc::ServerBuilder*>(builder);
      ::grpc::ResourceQuota quota("flight_test");
      quota.SetMaxThreads(max_threads);
      grpc_builder->SetResourceQuota(quota);
    };
  }

  auto status = server->Init(options);
  if (!status.ok()) {
    std::cerr << "Failed to init server: " << status << "\n";
    return 1;
  }

  int server_port = server->port();
  std::cout << "Server listening on port " << server_port << "\n";

  // Start server in background thread
  std::thread server_thread([&server]() {
    auto status = server->Serve();
    if (!status.ok()) {
      std::cerr << "Server error: " << status << "\n";
    }
  });

  // Give server time to start
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Create client
  auto client_location = flight::Location::ForGrpcTcp("127.0.0.1", server_port);
  if (!client_location.ok()) {
    std::cerr << "Failed to create client location: " << client_location.status() << "\n";
    return 1;
  }

  auto client_result = flight::FlightClient::Connect(*client_location);
  if (!client_result.ok()) {
    std::cerr << "Failed to connect: " << client_result.status() << "\n";
    return 1;
  }
  auto client = std::move(*client_result);

  std::cout << "\nSpawning " << num_clients << " concurrent DoExchange calls...\n\n";

  // Spawn many concurrent client calls
  std::vector<std::thread> client_threads;
  client_threads.reserve(num_clients);

  for (int i = 0; i < num_clients; ++i) {
    client_threads.emplace_back([&client_location, i]() {
      // Each thread needs its own client connection
      auto conn = flight::FlightClient::Connect(*client_location);
      if (!conn.ok()) {
        std::cerr << "Client " << i << " failed to connect\n";
        return;
      }
      auto status = MakeExchangeCall(conn->get());
      if (!status.ok()) {
        std::cerr << "Client " << i << " error: " << status << "\n";
      }
    });

    // Small delay to let connections establish
    if (i % 10 == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }

  // Wait for all clients to connect and handlers to be active
  std::this_thread::sleep_for(std::chrono::seconds(1));

  // Measure thread count while handlers are active
  int active_threads = GetThreadCount();
  int active_handlers = g_active_handlers.load();

  std::cout << "=== RESULTS (while handlers active) ===\n"
            << "Active DoExchange handlers: " << active_handlers << "\n"
            << "Peak concurrent handlers: " << g_peak_handlers.load() << "\n"
            << "Current thread count: " << active_threads << "\n"
            << "Threads created: " << (active_threads - baseline_threads) << "\n"
            << "Threads per handler (approx): "
            << (active_handlers > 0
                    ? (float)(active_threads - baseline_threads) / active_handlers
                    : 0)
            << "\n\n";

  if (active_threads - baseline_threads >= num_clients * 0.8) {
    std::cout << "CONFIRMED: Thread-per-request behavior detected!\n"
              << "Each DoExchange call is consuming approximately one thread.\n";
  }

  // Wait for all client threads to complete
  std::cout << "\nWaiting for all requests to complete...\n";
  for (auto& t : client_threads) {
    t.join();
  }

  // Measure thread count after completion
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  int final_threads = GetThreadCount();

  std::cout << "\n=== FINAL STATE ===\n"
            << "Thread count after completion: " << final_threads << "\n"
            << "Threads released: " << (active_threads - final_threads) << "\n";

  // Shutdown
  status = client->Close();
  if (!status.ok()) {
    std::cerr << "Failed to close client: " << status << "\n";
  }
  status = server->Shutdown();
  if (!status.ok()) {
    std::cerr << "Failed to shutdown server: " << status << "\n";
  }
  server_thread.join();

  std::cout << "\nTest complete.\n";
  return 0;
}
