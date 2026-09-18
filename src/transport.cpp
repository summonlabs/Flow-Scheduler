// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/transport.hpp"

#include <atomic>
#include <cstring>
#include <mutex>

#if defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#endif

namespace flow_scheduler {
namespace {

#if defined(_WIN32)
using NativeSocket = SOCKET;
constexpr NativeSocket kInvalidNative = INVALID_SOCKET;
#else
using NativeSocket = int;
constexpr NativeSocket kInvalidNative = -1;
#endif

std::atomic<int> g_transport_users{0};
std::mutex g_transport_mutex;

NativeSocket to_native(std::uintptr_t handle) noexcept {
  return static_cast<NativeSocket>(handle);
}

std::uintptr_t from_native(NativeSocket socket) noexcept {
  return static_cast<std::uintptr_t>(socket);
}

Status socket_failure(const char* operation, ErrorCode code = ErrorCode::TransportFailure) {
  std::string detail(operation);
  detail += " failed";
#if defined(_WIN32)
  detail += " (winsock error ";
  detail += std::to_string(WSAGetLastError());
  detail += ")";
#else
  detail += " (";
  detail += std::strerror(errno);
  detail += ")";
#endif
  return Status::failure(code, std::move(detail));
}

bool would_block() noexcept {
#if defined(_WIN32)
  const int error = WSAGetLastError();
  return error == WSAEWOULDBLOCK || error == WSAEINPROGRESS;
#else
  return errno == EWOULDBLOCK || errno == EAGAIN || errno == EINPROGRESS;
#endif
}

Status set_blocking(NativeSocket socket, bool blocking) noexcept {
#if defined(_WIN32)
  u_long mode = blocking ? 0ul : 1ul;
  if (ioctlsocket(socket, FIONBIO, &mode) != 0) {
    return socket_failure("ioctlsocket");
  }
#else
  const int flags = fcntl(socket, F_GETFL, 0);
  if (flags < 0) {
    return socket_failure("fcntl(F_GETFL)");
  }
  const int updated = blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK);
  if (fcntl(socket, F_SETFL, updated) < 0) {
    return socket_failure("fcntl(F_SETFL)");
  }
#endif
  return Status::success();
}

void close_native(NativeSocket socket) noexcept {
  if (socket == kInvalidNative) {
    return;
  }
#if defined(_WIN32)
  ::closesocket(socket);
#else
  ::close(socket);
#endif
}

}  // namespace

Status transport_init() {
  std::lock_guard<std::mutex> guard(g_transport_mutex);
#if defined(_WIN32)
  if (g_transport_users.load() == 0) {
    WSADATA data;
    const int result = WSAStartup(MAKEWORD(2, 2), &data);
    if (result != 0) {
      return Status::failure(ErrorCode::TransportFailure,
                             "WSAStartup failed with error " + std::to_string(result));
    }
  }
#endif
  g_transport_users.fetch_add(1);
  return Status::success();
}

void transport_shutdown() {
  std::lock_guard<std::mutex> guard(g_transport_mutex);
  if (g_transport_users.load() <= 0) {
    return;
  }
  if (g_transport_users.fetch_sub(1) == 1) {
#if defined(_WIN32)
    WSACleanup();
#endif
  }
}

// ---------------------------------------------------------------------------
// Socket
// ---------------------------------------------------------------------------

Socket::Socket(Socket&& other) noexcept : handle_(other.handle_) {
  other.handle_ = kInvalidHandle;
}

Socket& Socket::operator=(Socket&& other) noexcept {
  if (this != &other) {
    close();
    handle_ = other.handle_;
    other.handle_ = kInvalidHandle;
  }
  return *this;
}

Socket::~Socket() { close(); }

void Socket::close() noexcept {
  if (handle_ != kInvalidHandle) {
    close_native(to_native(handle_));
    handle_ = kInvalidHandle;
  }
}

void Socket::shutdown_send() noexcept {
  if (handle_ == kInvalidHandle) {
    return;
  }
#if defined(_WIN32)
  ::shutdown(to_native(handle_), SD_SEND);
#else
  ::shutdown(to_native(handle_), SHUT_WR);
#endif
}

void Socket::set_no_delay() noexcept {
  if (handle_ == kInvalidHandle) {
    return;
  }
  const int enable = 1;
  ::setsockopt(to_native(handle_), IPPROTO_TCP, TCP_NODELAY,
               reinterpret_cast<const char*>(&enable), static_cast<int>(sizeof(enable)));
}

void Socket::set_keep_alive() noexcept {
  if (handle_ == kInvalidHandle) {
    return;
  }
  const int enable = 1;
  ::setsockopt(to_native(handle_), SOL_SOCKET, SO_KEEPALIVE,
               reinterpret_cast<const char*>(&enable), static_cast<int>(sizeof(enable)));
}

Result<Socket> Socket::adopt(std::uintptr_t raw_handle) {
  if (raw_handle == kInvalidHandle) {
    return Error(ErrorCode::InvalidArgument, "cannot adopt an invalid socket handle");
  }
  Socket socket;
  socket.handle_ = raw_handle;
  return socket;
}

Result<Socket> Socket::connect(std::string_view host, std::uint16_t port,
                               std::uint32_t timeout_ms) {
  FS_RETURN_IF_ERROR(transport_init());
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  const std::string host_text(host);
  const std::string port_text = std::to_string(port);
  addrinfo* results = nullptr;
  const int resolved =
      ::getaddrinfo(host_text.c_str(), port_text.c_str(), &hints, &results);
  if (resolved != 0 || results == nullptr) {
    return Error(ErrorCode::TransportFailure, "cannot resolve the peer address");
  }
  Status last_error = Status::failure(ErrorCode::TransportFailure, "no usable address");
  for (addrinfo* candidate = results; candidate != nullptr; candidate = candidate->ai_next) {
    const NativeSocket native =
        ::socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
    if (native == kInvalidNative) {
      last_error = socket_failure("socket");
      continue;
    }
    const Status non_blocking = set_blocking(native, false);
    if (!non_blocking.ok()) {
      last_error = non_blocking;
      close_native(native);
      continue;
    }
    const int result = ::connect(native, candidate->ai_addr,
                                 static_cast<int>(candidate->ai_addrlen));
    bool connected = result == 0;
    if (!connected && would_block()) {
      fd_set writable;
      FD_ZERO(&writable);
      FD_SET(native, &writable);
      timeval timeout{};
      timeout.tv_sec = static_cast<long>(timeout_ms / 1000u);
      timeout.tv_usec = static_cast<long>((timeout_ms % 1000u) * 1000u);
      // The first argument is ignored on Windows and is the highest
      // descriptor plus one on POSIX, so a single portable call suffices.
      const int ready =
          ::select(static_cast<int>(native + 1), nullptr, &writable, nullptr, &timeout);
      if (ready > 0) {
        int socket_error = 0;
        socklen_t length = static_cast<socklen_t>(sizeof(socket_error));
        if (::getsockopt(native, SOL_SOCKET, SO_ERROR,
                         reinterpret_cast<char*>(&socket_error), &length) == 0 &&
            socket_error == 0) {
          connected = true;
        }
      } else if (ready == 0) {
        last_error = Status::failure(ErrorCode::TransportFailure, "connect timed out");
      }
    }
    if (!connected) {
      if (last_error.ok()) {
        last_error = socket_failure("connect");
      }
      close_native(native);
      continue;
    }
    const Status blocking = set_blocking(native, true);
    if (!blocking.ok()) {
      last_error = blocking;
      close_native(native);
      continue;
    }
    ::freeaddrinfo(results);
    Socket socket;
    socket.handle_ = from_native(native);
    socket.set_no_delay();
    socket.set_keep_alive();
    return socket;
  }
  ::freeaddrinfo(results);
  return last_error.error();
}

Status Socket::send_all(std::string_view bytes) {
  if (handle_ == kInvalidHandle) {
    return Status::failure(ErrorCode::InvalidArgument, "send on a closed socket");
  }
  const NativeSocket native = to_native(handle_);
  std::size_t sent = 0;
  while (sent < bytes.size()) {
    const std::size_t chunk = bytes.size() - sent;
    const int request = chunk > static_cast<std::size_t>(1u << 30) ? (1 << 30)
                                                                   : static_cast<int>(chunk);
    const int written = ::send(native, bytes.data() + sent, request, 0);
    if (written <= 0) {
      if (written < 0 && would_block()) {
        continue;
      }
      return socket_failure("send", ErrorCode::PeerClosed);
    }
    sent += static_cast<std::size_t>(written);
  }
  return Status::success();
}

Result<std::size_t> Socket::recv_some(char* buffer, std::size_t capacity) {
  if (handle_ == kInvalidHandle) {
    return Error(ErrorCode::InvalidArgument, "receive on a closed socket");
  }
  if (capacity == 0) {
    return Error(ErrorCode::InvalidArgument, "receive buffer must be non-empty");
  }
  const int request = capacity > static_cast<std::size_t>(1u << 30) ? (1 << 30)
                                                                    : static_cast<int>(capacity);
  const int received = ::recv(to_native(handle_), buffer, request, 0);
  if (received < 0) {
    if (would_block()) {
      return static_cast<std::size_t>(0);
    }
    return Error(ErrorCode::TransportFailure, "recv failed");
  }
  return static_cast<std::size_t>(received);
}

// ---------------------------------------------------------------------------
// Listener
// ---------------------------------------------------------------------------

Listener::Listener(Listener&& other) noexcept : handle_(other.handle_), port_(other.port_) {
  other.handle_ = kInvalidHandle;
  other.port_ = 0;
}

Listener& Listener::operator=(Listener&& other) noexcept {
  if (this != &other) {
    close();
    handle_ = other.handle_;
    port_ = other.port_;
    other.handle_ = kInvalidHandle;
    other.port_ = 0;
  }
  return *this;
}

Listener::~Listener() { close(); }

void Listener::close() noexcept {
  if (handle_ != kInvalidHandle) {
    close_native(to_native(handle_));
    handle_ = kInvalidHandle;
  }
}

Result<Listener> Listener::bind(std::string_view host, std::uint16_t port) {
  FS_RETURN_IF_ERROR(transport_init());
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = AI_PASSIVE;
  const std::string host_text(host);
  const std::string port_text = std::to_string(port);
  addrinfo* results = nullptr;
  const int resolved = ::getaddrinfo(host_text.empty() ? nullptr : host_text.c_str(),
                                     port_text.c_str(), &hints, &results);
  if (resolved != 0 || results == nullptr) {
    return Error(ErrorCode::TransportFailure, "cannot resolve the bind address");
  }
  Status last_error = Status::failure(ErrorCode::TransportFailure, "no usable bind address");
  for (addrinfo* candidate = results; candidate != nullptr; candidate = candidate->ai_next) {
    const NativeSocket native =
        ::socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
    if (native == kInvalidNative) {
      last_error = socket_failure("socket");
      continue;
    }
    const int enable = 1;
    ::setsockopt(native, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&enable),
                 static_cast<int>(sizeof(enable)));
    if (::bind(native, candidate->ai_addr, static_cast<int>(candidate->ai_addrlen)) != 0) {
      last_error = socket_failure("bind");
      close_native(native);
      continue;
    }
    if (::listen(native, 64) != 0) {
      last_error = socket_failure("listen");
      close_native(native);
      continue;
    }
    sockaddr_storage address{};
    socklen_t address_length = static_cast<socklen_t>(sizeof(address));
    if (::getsockname(native, reinterpret_cast<sockaddr*>(&address), &address_length) != 0) {
      last_error = socket_failure("getsockname");
      close_native(native);
      continue;
    }
    std::uint16_t bound_port = 0;
    if (address.ss_family == AF_INET) {
      const auto* ipv4 = reinterpret_cast<const sockaddr_in*>(&address);
      bound_port = ntohs(ipv4->sin_port);
    } else if (address.ss_family == AF_INET6) {
      const auto* ipv6 = reinterpret_cast<const sockaddr_in6*>(&address);
      bound_port = ntohs(ipv6->sin6_port);
    }
    ::freeaddrinfo(results);
    Listener listener;
    listener.handle_ = from_native(native);
    listener.port_ = bound_port;
    return listener;
  }
  ::freeaddrinfo(results);
  return last_error.error();
}

Result<Socket> Listener::accept() {
  if (handle_ == kInvalidHandle) {
    return Error(ErrorCode::InvalidArgument, "accept on a closed listener");
  }
  sockaddr_storage address{};
  socklen_t address_length = static_cast<socklen_t>(sizeof(address));
  const NativeSocket accepted =
      ::accept(to_native(handle_), reinterpret_cast<sockaddr*>(&address), &address_length);
  if (accepted == kInvalidNative) {
    return Error(ErrorCode::TransportFailure, "accept failed");
  }
  Result<Socket> socket = Socket::adopt(from_native(accepted));
  if (!socket.ok()) {
    close_native(accepted);
    return socket.error();
  }
  socket.value().set_no_delay();
  socket.value().set_keep_alive();
  return socket;
}

// ---------------------------------------------------------------------------
// wait_readable
// ---------------------------------------------------------------------------

Result<std::vector<std::size_t>> wait_readable(const std::vector<const Socket*>& sockets,
                                               std::uint32_t timeout_ms) {
  std::vector<std::size_t> ready;
  if (sockets.empty()) {
    return ready;
  }
  if (sockets.size() > static_cast<std::size_t>(FD_SETSIZE)) {
    return Error(ErrorCode::Bounded, "select cannot watch this many sockets");
  }
  fd_set readable;
  FD_ZERO(&readable);
  NativeSocket highest = 0;
  for (const Socket* socket : sockets) {
    if (socket == nullptr || !socket->valid()) {
      return Error(ErrorCode::InvalidArgument, "wait_readable received an invalid socket");
    }
    const NativeSocket native = to_native(socket->native_handle());
    FD_SET(native, &readable);
    if (native > highest) {
      highest = native;
    }
  }
  timeval timeout{};
  timeout.tv_sec = static_cast<long>(timeout_ms / 1000u);
  timeout.tv_usec = static_cast<long>((timeout_ms % 1000u) * 1000u);
  // The first argument is ignored on Windows and is the highest descriptor
  // plus one on POSIX, so a single portable call suffices.
  const int result = ::select(static_cast<int>(highest + 1), &readable, nullptr, nullptr,
                              &timeout);
  if (result < 0) {
    return Error(ErrorCode::TransportFailure, "select failed");
  }
  if (result == 0) {
    return ready;
  }
  for (std::size_t index = 0; index < sockets.size(); ++index) {
    const NativeSocket native = to_native(sockets[index]->native_handle());
    if (FD_ISSET(native, &readable) != 0) {
      ready.push_back(index);
    }
  }
  return ready;
}

// ---------------------------------------------------------------------------
// FrameChannel
// ---------------------------------------------------------------------------

Status FrameChannel::send_frame(std::uint16_t type, std::string_view payload) {
  std::string encoded;
  FS_TRY_ASSIGN(encoded, encode_frame(type, payload));
  return socket_.send_all(encoded);
}

Result<bool> FrameChannel::read_frame(std::uint16_t& type, std::string& payload) {
  for (;;) {
    FrameHeader header;
    std::string body;
    bool have_frame = false;
    FS_TRY_ASSIGN(have_frame, accumulator_.next(header, body));
    if (have_frame) {
      type = header.type;
      payload = std::move(body);
      return true;
    }
    // A modest receive buffer keeps this frame off the static-analysis stack
    // budget; the loop below already handles frames larger than one read.
    char buffer[8192];
    std::size_t received = 0;
    FS_TRY_ASSIGN(received, socket_.recv_some(buffer, sizeof(buffer)));
    if (received == 0) {
      if (accumulator_.buffered() == 0) {
        return false;  // Orderly close at a frame boundary.
      }
      return Error(ErrorCode::Truncated, "peer closed in the middle of a frame");
    }
    FS_RETURN_IF_ERROR(accumulator_.push(std::string_view(buffer, received)));
  }
}

}  // namespace flow_scheduler
