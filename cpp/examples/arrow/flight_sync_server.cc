// Sync Flight server - demonstrates thread-per-request behavior
// Usage: ./flight-sync-server [port]

#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>
#include "arrow/flight/api.h"

std::atomic<int> g_active{0};
std::atomic<int> g_peak{0};

int GetThreadCount() {
  std::ifstream f("/proc/self/status");
  std::string field;
  while (f >> field) {
    if (field == "Threads:") {
      int n;
      f >> n;
      return n;
    }
  }
  return -1;
}

class SyncServer : public arrow::flight::FlightServerBase {
 public:
  arrow::Status DoExchange(
      const arrow::flight::ServerCallContext&,
      std::unique_ptr<arrow::flight::FlightMessageReader>,
      std::unique_ptr<arrow::flight::FlightMessageWriter> writer) override {
    int cur = ++g_active;
    int peak = g_peak.load();
    while (cur > peak && !g_peak.compare_exchange_weak(peak, cur)) {
    }

    // Hold for 30 seconds (BLOCKING - this is the problem)
    std::this_thread::sleep_for(std::chrono::seconds(30));

    --g_active;
    return writer->Close();
  }
};

int main(int argc, char** argv) {
  int port = argc > 1 ? std::stoi(argv[1]) : 50051;

  std::cout << "=== SYNC Flight Server ===\n";
  std::cout << "Port: " << port << "\n";
  std::cout << "Each DoExchange holds for 30 seconds (BLOCKING)\n\n";

  SyncServer server;
  auto location = arrow::flight::Location::ForGrpcTcp("0.0.0.0", port);
  if (!location.ok()) {
    std::cerr << "Failed to create location: " << location.status() << "\n";
    return 1;
  }

  arrow::flight::FlightServerOptions options(*location);
  auto status = server.Init(options);
  if (!status.ok()) {
    std::cerr << "Failed to init server: " << status << "\n";
    return 1;
  }

  std::cout << "Server started. Baseline threads: " << GetThreadCount() << "\n\n";

  // Stats printer
  std::thread stats([&]() {
    while (true) {
      std::this_thread::sleep_for(std::chrono::seconds(2));
      std::cout << "Active: " << g_active << " | Peak: " << g_peak
                << " | Threads: " << GetThreadCount() << "\n";
    }
  });
  stats.detach();

  status = server.Serve();
  if (!status.ok()) {
    std::cerr << "Server error: " << status << "\n";
    return 1;
  }
  return 0;
}
