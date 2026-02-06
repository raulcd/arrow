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

// Minimal Flight client for thread count validation.
// Opens 1000 concurrent DoGet streams and holds them open.

#include <iostream>
#include <vector>

#include "arrow/flight/client.h"

constexpr int kNumStreams = 1000;

int main() {
  auto location = arrow::flight::Location::Parse("grpc://localhost:31337").ValueOrDie();
  auto client = arrow::flight::FlightClient::Connect(location).ValueOrDie();

  std::cout << "Opening " << kNumStreams << " streams..." << std::endl;

  std::vector<std::unique_ptr<arrow::flight::FlightStreamReader>> readers;
  for (int i = 0; i < kNumStreams; ++i) {
    arrow::flight::Ticket ticket{"stream-" + std::to_string(i)};
    auto reader = client->DoGet(ticket).ValueOrDie();
    readers.push_back(std::move(reader));
    if ((i + 1) % 100 == 0) {
      std::cout << "Opened " << (i + 1) << " streams..." << std::endl;
    }
  }

  std::cout << "All " << kNumStreams << " streams open. Press Enter to drain..."
            << std::endl;
  std::cin.get();

  std::cout << "Draining streams..." << std::endl;
  for (auto& reader : readers) {
    while (true) {
      auto chunk = reader->Next().ValueOrDie();
      if (!chunk.data) break;
    }
  }

  std::cout << "Done." << std::endl;
  auto status = client->Close();
  if (!status.ok()) {
    std::cerr << "Error closing client: " << status.ToString() << std::endl;
    return 1;
  }
}
