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

/// \file async_grpc_server.h
/// \brief PoC: Async Flight DoGet server using gRPC's generic callback API.
///
/// This implements a Flight-compatible gRPC server that uses
/// CallbackGenericService + ServerGenericBidiReactor instead of the
/// synchronous FlightService::Service. ByteBuffers are serialized/deserialized
/// directly via FlightDataSerialize/FlightDataDeserialize, bypassing the
/// SerializationTraits type-punning hack.

#pragma once

#include <functional>
#include <memory>
#include <string>

#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/byte_buffer.h>

#include "arrow/buffer.h"
#include "arrow/flight/protocol_internal.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/transport/grpc/customize_grpc.h"
#include "arrow/flight/types.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"

namespace arrow::flight::transport::grpc {

namespace pb = arrow::flight::protocol;

// ---------------------------------------------------------------------------
// DoGet reactor: reads a Ticket, streams back schema + RecordBatches
// ---------------------------------------------------------------------------

/// Callback for the DoGet reactor to obtain FlightPayloads.
///
/// The PoC uses a simple function to produce payloads, but this could
/// be replaced by a FlightDataStream or an async handler in a real
/// implementation.
///
/// Return a payload with ipc_message.metadata == nullptr to signal end of stream.
using PayloadProducer = std::function<arrow::Result<FlightPayload>()>;

/// Reactor that handles a DoGet RPC via the generic callback API.
///
/// Lifecycle:
///   1. Constructor calls StartRead() to get the Ticket.
///   2. OnReadDone: deserialize Ticket, create a PayloadProducer, write first payload.
///   3. OnWriteDone: pull next payload from producer, write or finish.
///   4. OnDone: self-delete.
class DoGetReactor : public ::grpc::ServerGenericBidiReactor {
 public:
  /// \param make_producer Factory called with the deserialized Ticket.
  ///   Returns a PayloadProducer that yields schema + batch payloads.
  using ProducerFactory =
      std::function<arrow::Result<PayloadProducer>(const arrow::flight::Ticket&)>;

  explicit DoGetReactor(ProducerFactory factory) : factory_(std::move(factory)) {
    StartRead(&request_buf_);
  }

  void OnReadDone(bool ok) override {
    if (!ok) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL, "Failed to read request"));
      return;
    }

    // Deserialize Ticket from ByteBuffer
    ::grpc::Slice slice;
    // This might potentially copy data if more than one slice is present
    auto grpc_status = request_buf_.DumpToSingleSlice(&slice);
    if (!grpc_status.ok()) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL,
                            "Failed to dump ByteBuffer to slice"));
      return;
    }

    pb::Ticket pb_ticket;
    if (!pb_ticket.ParseFromArray(slice.begin(), static_cast<int>(slice.size()))) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL, "Failed to parse Ticket"));
      return;
    }

    arrow::flight::Ticket ticket;
    auto status = arrow::flight::internal::FromProto(pb_ticket, &ticket);
    if (!status.ok()) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL, status.ToString()));
      return;
    }

    // Create the payload producer from the factory
    auto producer_result = factory_(ticket);
    if (!producer_result.ok()) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL,
                            producer_result.status().ToString()));
      return;
    }
    producer_ = std::move(*producer_result);

    // Write the first payload (should be the schema)
    WriteNextPayload();
  }

  void OnWriteDone(bool ok) override {
    if (!ok) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL, "Write failed"));
      return;
    }
    WriteNextPayload();
  }

  void OnCancel() override {
    // Client cancelled the RPC. Cleanup happens in OnDone().
  }

  void OnDone() override { delete this; }

 private:
  void WriteNextPayload() {
    // NOTE: This calls the user's producer, which is synchronous.
    // If producer is slow (e.g., generating large batches, complex computation),
    // this wastes a thread from the gRPC pool. For true async producers,
    // would need to extend this to return futures.
    auto payload_result = producer_();
    if (!payload_result.ok()) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL,
                            payload_result.status().ToString()));
      return;
    }

    auto payload = std::move(*payload_result);
    // End of stream signaled by null metadata
    if (payload.ipc_message.metadata == nullptr) {
      Finish(::grpc::Status::OK);
      return;
    }

    bool own_buffer = false;
    auto grpc_status = FlightDataSerialize(payload, &write_buf_, &own_buffer);
    if (!grpc_status.ok()) {
      Finish(grpc_status);
      return;
    }

    StartWrite(&write_buf_);
  }

  ProducerFactory factory_;
  PayloadProducer producer_;

  ::grpc::ByteBuffer request_buf_;
  ::grpc::ByteBuffer write_buf_;
};

// ---------------------------------------------------------------------------
// DoPut reactor: reads FlightData stream, writes back PutResult
// ---------------------------------------------------------------------------

/// Callback for the DoPut reactor to consume FlightData messages.
///
/// Called for each incoming FlightData message. Return Status::OK to continue
/// reading, or an error to abort.
using DataConsumer = std::function<arrow::Status(arrow::flight::internal::FlightData)>;

/// Reactor that handles a DoPut RPC via the generic callback API.
///
/// Lifecycle:
///   1. Constructor calls StartRead() to receive the first FlightData.
///   2. OnReadDone: deserialize FlightData, pass to consumer, read next or finish.
///   3. OnWriteDone: cleanup after sending PutResult.
///   4. OnDone: self-delete.
class DoPutReactor : public ::grpc::ServerGenericBidiReactor {
 public:
  explicit DoPutReactor(DataConsumer consumer) : consumer_(std::move(consumer)) {
    StartRead(&read_buf_);
  }

  void OnReadDone(bool ok) override {
    if (!ok) {
      // End of stream from client - send empty PutResult to acknowledge completion
      pb::PutResult pb_result;
      // Use Slice(string) constructor which copies data, avoiding use-after-free
      ::grpc::Slice slice(pb_result.SerializeAsString());
      write_buf_ = ::grpc::ByteBuffer(&slice, 1);
      StartWrite(&write_buf_);
      return;
    }

    // Deserialize FlightData from ByteBuffer
    arrow::flight::internal::FlightData flight_data;
    auto status = FlightDataDeserialize(&read_buf_, &flight_data);
    if (!status.ok()) {
      Finish(status);
      return;
    }

    // Pass to consumer
    // NOTE: This is a synchronous call that blocks until consumer_ returns.
    // If the consumer is slow (e.g., blocking I/O), this wastes a thread from the gRPC
    // pool. For true async I/O in the consumer, would need to extend this to return
    // futures.
    auto consume_status = consumer_(std::move(flight_data));
    if (!consume_status.ok()) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL, consume_status.ToString()));
      return;
    }

    // Read next FlightData
    StartRead(&read_buf_);
  }

  void OnWriteDone(bool ok) override {
    if (!ok) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL, "Write failed"));
      return;
    }
    Finish(::grpc::Status::OK);
  }

  void OnCancel() override {
    // Client cancelled the RPC. Cleanup happens in OnDone().
  }

  void OnDone() override { delete this; }

 private:
  DataConsumer consumer_;

  ::grpc::ByteBuffer read_buf_;
  ::grpc::ByteBuffer write_buf_;
};

// ---------------------------------------------------------------------------
// Generic callback service that dispatches by method name
// ---------------------------------------------------------------------------

/// A CallbackGenericService that routes Flight RPCs to reactors.
///
/// Currently DoGet and DoPut are implemented (for the PoC).
class FlightCallbackService : public ::grpc::CallbackGenericService {
 public:
  /// Set the factory used to create DoGet payload producers.
  void SetDoGetFactory(DoGetReactor::ProducerFactory factory) {
    do_get_factory_ = std::move(factory);
  }

  /// Set the consumer used to handle DoPut payload data.
  void SetDoPutConsumer(DataConsumer consumer) { do_put_consumer_ = std::move(consumer); }

  ::grpc::ServerGenericBidiReactor* CreateReactor(
      ::grpc::GenericCallbackServerContext* ctx) override {
    const std::string& method = ctx->method();

    if (method == "/arrow.flight.protocol.FlightService/DoGet") {
      if (do_get_factory_) {
        return new DoGetReactor(do_get_factory_);
      }
    }

    if (method == "/arrow.flight.protocol.FlightService/DoPut") {
      if (do_put_consumer_) {
        return new DoPutReactor(do_put_consumer_);
      }
    }

    // Reject unknown or unconfigured methods
    class UnimplementedReactor : public ::grpc::ServerGenericBidiReactor {
     public:
      UnimplementedReactor() {
        Finish(::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED,
                              "Method not implemented in async PoC"));
      }
      void OnDone() override { delete this; }
    };
    return new UnimplementedReactor();
  }

 private:
  DoGetReactor::ProducerFactory do_get_factory_;
  DataConsumer do_put_consumer_;
};

}  // namespace arrow::flight::transport::grpc
