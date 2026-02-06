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

// Minimal async Flight server for thread count validation.

#include <dirent.h>
#include <chrono>
#include <iostream>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "arrow/flight/transport/grpc/async_grpc_server.h"

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

int main() {
  arrow::flight::transport::grpc::FlightCallbackService service;

  ::grpc::ServerBuilder builder;
  builder.AddListeningPort("0.0.0.0:31337", ::grpc::InsecureServerCredentials());
  builder.RegisterCallbackGenericService(&service);

  // Note: gRPC 1.46.3's callback executor thread pool is not easily configurable.
  // ResourceQuota::SetMaxThreads() only affects the sync server.
  // Thread count scales to ~2×nproc and doesn't shrink after load.
  // For tighter control, consider upgrading to gRPC 1.50+ with EventEngine.

  auto server = builder.BuildAndStart();
  if (!server) {
    std::cerr << "Failed to start server" << std::endl;
    return 1;
  }

  std::cout << "Async server listening on port 31337" << std::endl;

  while (true) {
    std::cout << "Threads: " << CountThreads() << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}
