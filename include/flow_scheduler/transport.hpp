// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_TRANSPORT_HPP
#define FLOW_SCHEDULER_TRANSPORT_HPP

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "flow_scheduler/error.hpp"
#include "flow_scheduler/framing.hpp"

namespace flow_scheduler {

/// One-time process-wide transport initialization (Winsock on Windows; a no-op
/// elsewhere). Idempotent and thread safe.
[[nodiscard]] Status transport_init();
void transport_shutdown();

/// Blocking stream socket. Every operation is bounded: sends loop until the
/// whole buffer is written or the peer fails; receives never allocate more
/// than the caller's buffer.
class Socket {
 public:
  Socket() noexcept = default;
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;
  Socket(Socket&& other) noexcept;
  Socket& operator=(Socket&& other) noexcept;
  ~Socket();

  [[nodiscard]] static Result<Socket> connect(std::string_view host, std::uint16_t port,
                                              std::uint32_t timeout_ms);
  [[nodiscard]] static Result<Socket> adopt(std::uintptr_t raw_handle);

  [[nodiscard]] bool valid() const noexcept { return handle_ != kInvalidHandle; }
  [[nodiscard]] std::uintptr_t native_handle() const noexcept { return handle_; }

  [[nodiscard]] Status send_all(std::string_view bytes);
  /// Returns the number of bytes read, 0 on orderly peer close, or an error.
  [[nodiscard]] Result<std::size_t> recv_some(char* buffer, std::size_t capacity);

  /// Half-close the send direction so the peer observes an orderly end of
  /// stream without tearing down the receive direction.
  void shutdown_send() noexcept;
  void close() noexcept;

  /// Disable Nagle so that latency measurements are not dominated by delayed
  /// acknowledgement. Best effort.
  void set_no_delay() noexcept;
  void set_keep_alive() noexcept;

 private:
  static constexpr std::uintptr_t kInvalidHandle = static_cast<std::uintptr_t>(~0ull);
  std::uintptr_t handle_{kInvalidHandle};
};

/// Listening socket bound to a loopback or wildcard address.
class Listener {
 public:
  Listener() noexcept = default;
  Listener(const Listener&) = delete;
  Listener& operator=(const Listener&) = delete;
  Listener(Listener&& other) noexcept;
  Listener& operator=(Listener&& other) noexcept;
  ~Listener();

  /// Bind to the given host and port. Port 0 requests an ephemeral port, which
  /// is then reported by port().
  [[nodiscard]] static Result<Listener> bind(std::string_view host, std::uint16_t port);

  [[nodiscard]] Result<Socket> accept();
  [[nodiscard]] std::uint16_t port() const noexcept { return port_; }
  [[nodiscard]] bool valid() const noexcept { return handle_ != kInvalidHandle; }
  void close() noexcept;

 private:
  static constexpr std::uintptr_t kInvalidHandle = static_cast<std::uintptr_t>(~0ull);
  std::uintptr_t handle_{kInvalidHandle};
  std::uint16_t port_{0};
};

/// Wait until at least one socket is readable or the timeout expires. Returns
/// the indices of every socket ready for reading; a socket whose peer closed is
/// reported ready so the caller observes the end of stream. A timeout is not an
/// error and yields an empty result.
[[nodiscard]] Result<std::vector<std::size_t>> wait_readable(
    const std::vector<const Socket*>& sockets, std::uint32_t timeout_ms);

/// Blocking frame reader on top of a Socket. Enforces the framing bound and
/// distinguishes an orderly close from a torn read.
///
/// Ownership contract: a FrameChannel is a *single blocking reader* for its
/// socket. One receive may carry several coalesced frames, and every frame
/// after the first is retained in the channel's private accumulator. A caller
/// that multiplexes several sockets through select() must therefore either use
/// one FrameChannel per socket and always drain it completely, or compose
/// Socket + FrameAccumulator itself so that buffered frames are delivered
/// before the socket is polled again. Reading a multiplexed socket through
/// this class can strand frames that select() will never report.
class FrameChannel {
 public:
  FrameChannel() = default;
  explicit FrameChannel(Socket socket) noexcept : socket_(std::move(socket)) {}

  [[nodiscard]] bool valid() const noexcept { return socket_.valid(); }

  [[nodiscard]] Status send_frame(std::uint16_t type, std::string_view payload);

  /// Read one frame. A clean end of stream before any byte of a new frame is
  /// reported as PeerClosed; an end of stream in the middle of a frame is
  /// reported as Truncated.
  [[nodiscard]] Result<bool> read_frame(std::uint16_t& type, std::string& payload);

  [[nodiscard]] Socket& socket() noexcept { return socket_; }
  void close() noexcept { socket_.close(); }

 private:
  Socket socket_{};
  FrameAccumulator accumulator_{};
};

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_TRANSPORT_HPP
