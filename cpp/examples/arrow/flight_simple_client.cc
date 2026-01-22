// Simple Flight client - connects N clients to a server
// Usage: ./flight-simple-client [port] [num_clients]

#include <iostream>
#include <thread>
#include <vector>
#include "arrow/flight/api.h"

int main(int argc, char** argv) {
  int port = argc > 1 ? std::stoi(argv[1]) : 50051;
  int num_clients = argc > 2 ? std::stoi(argv[2]) : 100;

  std::cout << "Connecting " << num_clients << " clients to port " << port << "\n";

  auto location = *arrow::flight::Location::ForGrpcTcp("127.0.0.1", port);

  std::vector<std::thread> threads;
  for (int i = 0; i < num_clients; ++i) {
    threads.emplace_back([&location, i]() {
      auto client = *arrow::flight::FlightClient::Connect(location);
      auto descriptor = arrow::flight::FlightDescriptor::Command("test");
      auto exchange = client->DoExchange(descriptor);
      if (exchange.ok()) {
        // Just hold the connection open, read until server closes
        while (true) {
          auto chunk = exchange->reader->Next();
          if (!chunk.ok() || (!chunk->data && !chunk->app_metadata)) break;
        }
      }
      std::cout << "Client " << i << " done\n";
    });
  }

  for (auto& t : threads) t.join();
  std::cout << "All clients done\n";
  return 0;
}
