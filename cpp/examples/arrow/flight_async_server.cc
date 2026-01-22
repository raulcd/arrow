// Async Flight server - demonstrates callback-based threading
// Usage: ./flight-async-server [port]
//
// NOTE: This is a simplified PoC that uses raw gRPC without Arrow's custom
// serialization. It demonstrates the threading model improvement.

#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/byte_buffer.h>

// Use the raw generated proto service (without Arrow's custom serialization)
#include "arrow/flight/Flight.pb.h"
#include "arrow/flight/Flight.grpc.pb.h"

namespace pb = arrow::flight::protocol;

std::atomic<int> g_active{0};
std::atomic<int> g_peak{0};

int GetThreadCount() {
  std::ifstream f("/proc/self/status");
  std::string field;
  while (f >> field) {
    if (field == "Threads:") { int n; f >> n; return n; }
  }
  return -1;
}

// Async reactor - holds connection for 30 seconds WITHOUT blocking a thread
class AsyncDoExchangeReactor : public grpc::ServerBidiReactor<grpc::ByteBuffer, grpc::ByteBuffer> {
 public:
  AsyncDoExchangeReactor() : start_(std::chrono::steady_clock::now()) {
    int cur = ++g_active;
    int peak = g_peak.load();
    while (cur > peak && !g_peak.compare_exchange_weak(peak, cur)) {}
    StartRead(&request_);
  }

  void OnReadDone(bool ok) override {
    if (ok) {
      auto elapsed = std::chrono::steady_clock::now() - start_;
      if (elapsed < std::chrono::seconds(30)) {
        StartRead(&request_);
      } else {
        Finish(grpc::Status::OK);
      }
    } else {
      Finish(grpc::Status::OK);
    }
  }

  void OnWriteDone(bool) override {}

  void OnDone() override {
    --g_active;
    delete this;
  }

 private:
  grpc::ByteBuffer request_;
  std::chrono::steady_clock::time_point start_;
};

// Use the raw callback service (ByteBuffer based)
class AsyncFlightService final : public pb::FlightService::RawCallbackService {
 public:
  grpc::ServerBidiReactor<grpc::ByteBuffer, grpc::ByteBuffer>* DoExchange(
      grpc::CallbackServerContext*) override {
    return new AsyncDoExchangeReactor();
  }

  // Stub other methods
  grpc::ServerBidiReactor<grpc::ByteBuffer, grpc::ByteBuffer>* Handshake(
      grpc::CallbackServerContext*) override { return nullptr; }
  grpc::ServerWriteReactor<grpc::ByteBuffer>* ListFlights(
      grpc::CallbackServerContext*, const grpc::ByteBuffer*) override { return nullptr; }
  grpc::ServerUnaryReactor* GetFlightInfo(grpc::CallbackServerContext* ctx,
      const grpc::ByteBuffer*, grpc::ByteBuffer*) override {
    auto* r = ctx->DefaultReactor();
    r->Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED, ""));
    return r;
  }
  grpc::ServerUnaryReactor* PollFlightInfo(grpc::CallbackServerContext* ctx,
      const grpc::ByteBuffer*, grpc::ByteBuffer*) override {
    auto* r = ctx->DefaultReactor();
    r->Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED, ""));
    return r;
  }
  grpc::ServerUnaryReactor* GetSchema(grpc::CallbackServerContext* ctx,
      const grpc::ByteBuffer*, grpc::ByteBuffer*) override {
    auto* r = ctx->DefaultReactor();
    r->Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED, ""));
    return r;
  }
  grpc::ServerWriteReactor<grpc::ByteBuffer>* DoGet(grpc::CallbackServerContext*,
      const grpc::ByteBuffer*) override { return nullptr; }
  grpc::ServerBidiReactor<grpc::ByteBuffer, grpc::ByteBuffer>* DoPut(
      grpc::CallbackServerContext*) override { return nullptr; }
  grpc::ServerWriteReactor<grpc::ByteBuffer>* ListActions(grpc::CallbackServerContext*,
      const grpc::ByteBuffer*) override { return nullptr; }
  grpc::ServerWriteReactor<grpc::ByteBuffer>* DoAction(grpc::CallbackServerContext*,
      const grpc::ByteBuffer*) override { return nullptr; }
};

int main(int argc, char** argv) {
  int port = argc > 1 ? std::stoi(argv[1]) : 50052;

  std::cout << "=== ASYNC Flight Server (Raw gRPC) ===\n";
  std::cout << "Port: " << port << "\n";
  std::cout << "Each DoExchange holds for 30 seconds (NON-BLOCKING)\n\n";

  AsyncFlightService service;
  grpc::ServerBuilder builder;
  builder.AddListeningPort("0.0.0.0:" + std::to_string(port),
                           grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  auto server = builder.BuildAndStart();
  if (!server) {
    std::cerr << "Failed to start server\n";
    return 1;
  }

  std::cout << "Server started. Baseline threads: " << GetThreadCount() << "\n\n";

  std::thread stats([&]() {
    while (true) {
      std::this_thread::sleep_for(std::chrono::seconds(2));
      std::cout << "Active: " << g_active << " | Peak: " << g_peak
                << " | Threads: " << GetThreadCount() << "\n";
    }
  });
  stats.detach();

  server->Wait();
  return 0;
}
