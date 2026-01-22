# Arrow Flight Async API Investigation

## Problem Statement

A customer requested an async API for Arrow Flight, specifically for `DoExchange`. The current implementation creates too many threads at scale - one thread per active request (thread-per-request model).

## Current Behavior Observed

Using `flight_thread_test.cc`, we confirmed:
- 100 concurrent `DoExchange` calls result in ~318 threads
- Each blocking call holds a thread for the duration of the request
- This doesn't scale for long-running streaming connections

## gRPC Threading Models

gRPC offers two main APIs:

1. **Sync API** (current Arrow Flight): Thread-per-request, blocking calls
2. **Callback/Reactor API**: Non-blocking, callback-based, no thread-per-request

The callback API uses `ServerBidiReactor<Request, Response>` for bidirectional streaming like `DoExchange`.

## How Arrow's Custom Serialization Works

Arrow Flight uses a clever hack to achieve zero-copy serialization. Key files:
- `cpp/src/arrow/flight/transport/grpc/customize_grpc.h`
- `cpp/src/arrow/flight/transport/grpc/serialization_internal.cc`

### The Hack

Arrow specializes `grpc::SerializationTraits<pb::FlightData>` (customize_grpc.h:96-116):

```cpp
template <>
class SerializationTraits<arrow::flight::protocol::FlightData> {
 public:
  static Status Serialize(const MessageType& msg, ByteBuffer* bb, bool* own_buffer) {
    return FlightDataSerialize(
        *reinterpret_cast<const arrow::flight::FlightPayload*>(&msg), bb, own_buffer);
  }

  static Status Deserialize(ByteBuffer* buffer, MessageType* msg) {
    return FlightDataDeserialize(
        buffer, reinterpret_cast<arrow::flight::internal::FlightData*>(msg));
  }
};
```

### How Sync API Uses This

1. Arrow NEVER creates actual `pb::FlightData` protobuf objects
2. Arrow creates its own objects (`FlightPayload` for writing, `internal::FlightData` for reading)
3. When writing, Arrow casts its object to `pb::FlightData*`:
   ```cpp
   // serialization_internal.cc:425
   return writer->Write(*reinterpret_cast<const pb::FlightData*>(&payload), ...);
   ```
4. gRPC calls `SerializationTraits::Serialize()`, which casts BACK to Arrow type
5. `FlightDataSerialize()` writes Arrow buffers directly to gRPC slices - zero copy

**Key insight**: Arrow controls both ends of the pointer cast. It creates the object, casts it to protobuf type, gRPC passes it through, and Arrow's custom trait casts it back.

## Why Callback API Fails

With the callback API, gRPC manages message lifecycle differently:

1. For `ServerBidiReactor<pb::FlightData, pb::FlightData>`, gRPC internally creates and manages `pb::FlightData` objects
2. When deserializing, gRPC passes a pointer to a REAL `pb::FlightData` object
3. Arrow's custom trait does `reinterpret_cast<internal::FlightData*>(msg)`
4. But `pb::FlightData` (protobuf) and `internal::FlightData` (Arrow) have completely different memory layouts
5. **CRASH** - memory corruption (`free(): invalid pointer`)

**Root cause**: Arrow's hack only works when Arrow controls both ends of the pointer cast. The callback API has gRPC create the message objects, breaking the assumption.

## Attempted PoC

Created three files:
- `flight_sync_server.cc` - Works, demonstrates thread-per-request problem
- `flight_async_server.cc` - Crashes due to serialization issue
- `flight_simple_client.cc` - Simple client to test both servers

The sync server successfully shows ~100 threads for 100 concurrent connections.
The async server crashes when clients connect due to the serialization mismatch.

## Possible Solutions

### Option 1: RawCallbackService with ByteBuffer + Manual Serialization

gRPC's callback API offers `ServerBidiReactor<ByteBuffer, ByteBuffer>` via `RawCallbackService`, which bypasses `SerializationTraits` entirely.

Arrow could:
- Create a new callback-based service handler using `ByteBuffer` directly
- Manually call `FlightDataSerialize`/`FlightDataDeserialize` on the raw bytes
- This preserves zero-copy but requires new code paths in Arrow Flight

This is the most viable option as it:
- Preserves Arrow's zero-copy optimization
- Uses gRPC's supported callback API
- Doesn't require changes to gRPC itself

### Option 2: Custom Message Allocator

gRPC has mechanisms for custom message allocators. Arrow could potentially intercept message creation to return Arrow objects instead of protobuf objects.

Challenges:
- Complex to implement
- May not be fully supported by gRPC
- Could break with gRPC updates

### Option 3: Accept Copies for Async

Use standard protobuf serialization for async use cases.

Drawbacks:
- Defeats the purpose of Arrow's zero-copy optimization
- Performance regression for async users

## Relevant GitHub Issues

- #34221: Request for async Flight API
- #34607: Discussion of async support
- #37937: Related async API discussions

Maintainer (David Li) was skeptical about building full async support due to complexity.

## Next Steps

1. Prototype Option 1: `RawCallbackService` with manual serialization
2. Measure performance to ensure zero-copy is preserved
3. Design API surface for async Flight server
4. Consider whether this should be opt-in or replace sync API

## Files Created/Modified

- `cpp/examples/arrow/flight_thread_test.cc` - Demonstrates thread-per-request
- `cpp/examples/arrow/flight_sync_server.cc` - Sync server PoC
- `cpp/examples/arrow/flight_async_server.cc` - Async server PoC (crashes)
- `cpp/examples/arrow/flight_simple_client.cc` - Test client
- `cpp/examples/arrow/CMakeLists.txt` - Build rules for examples
