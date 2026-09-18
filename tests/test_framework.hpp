// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.
//
// Minimal, dependency-free test harness. Tests are plain functions registered
// at static initialization time; main() runs them and reports a summary.
// Deliberately no timeouts anywhere: a hung test is a defect to diagnose.

#ifndef FLOW_SCHEDULER_TEST_FRAMEWORK_HPP
#define FLOW_SCHEDULER_TEST_FRAMEWORK_HPP

#include <atomic>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace fls_test {

struct TestCase {
  const char* suite;
  const char* name;
  void (*body)();
};

struct CaseFailure {
  std::string message;
};

inline std::vector<TestCase>& registry() {
  static std::vector<TestCase> cases;
  return cases;
}

inline std::atomic<long long>& check_count() {
  static std::atomic<long long> checks{0};
  return checks;
}

struct Registrar {
  Registrar(const char* suite, const char* name, void (*body)()) {
    registry().push_back(TestCase{suite, name, body});
  }
};

[[noreturn]] inline void raise(const char* file, int line, const std::string& message) {
  std::ostringstream stream;
  stream << file << '(' << line << "): " << message;
  throw CaseFailure{stream.str()};
}

inline void record_check(bool passed, const char* file, int line, const std::string& message) {
  check_count().fetch_add(1, std::memory_order_relaxed);
  if (!passed) {
    raise(file, line, message);
  }
}

template <class T, class = void>
struct is_streamable : std::false_type {};

template <class T>
struct is_streamable<
    T, std::void_t<decltype(std::declval<std::ostream&>() << std::declval<const T&>())>>
    : std::true_type {};

/// Renders a value for a failure message. Anything streamable is streamed;
/// enumerations fall back to their numeric value; everything else prints a
/// placeholder. Test suites add dedicated overloads for their identity types.
template <class T>
std::string show(const T& value) {
  if constexpr (is_streamable<T>::value) {
    std::ostringstream stream;
    stream << value;
    return stream.str();
  } else if constexpr (std::is_enum_v<T>) {
    return std::to_string(static_cast<long long>(value));
  } else {
    return std::string("<") + typeid(T).name() + ">";
  }
}

inline std::string show(bool value) { return value ? "true" : "false"; }
inline std::string show(std::uint8_t value) { return std::to_string(static_cast<unsigned>(value)); }

/// Uniform view over anything that reports success with ok() and an optional
/// to_string(). This lets the assertion macros accept both Status and
/// Result<T> without the framework depending on the library headers.
template <class T, class = void>
struct has_to_string : std::false_type {};

template <class T>
struct has_to_string<T, std::void_t<decltype(std::declval<const T&>().to_string())>>
    : std::true_type {};

struct StatusView {
  bool ok{false};
  std::string text{};
};

template <class T>
StatusView status_view(const T& value) {
  StatusView view;
  view.ok = static_cast<bool>(value.ok());
  if constexpr (has_to_string<T>::value) {
    view.text = value.to_string();
  } else {
    view.text = view.ok ? "ok" : "failure (no message)";
  }
  return view;
}

/// Error code of any status-like value; both Status and Result<T> expose it.
template <class T>
auto status_code(const T& value) {
  return value.code();
}

inline int run_all(int argc, char** argv) {
  std::string filter;
  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    if (argument == "--filter" && index + 1 < argc) {
      filter = argv[++index];
    } else if (argument.rfind("--filter=", 0) == 0) {
      filter = argument.substr(9);
    }
  }
  int passed = 0;
  int failed = 0;
  std::vector<std::string> failures;
  for (const TestCase& test : registry()) {
    const std::string full = std::string(test.suite) + "." + test.name;
    if (!filter.empty() && full.find(filter) == std::string::npos) {
      continue;
    }
    try {
      test.body();
      ++passed;
    } catch (const CaseFailure& failure) {
      ++failed;
      failures.push_back(full + "\n    " + failure.message);
    } catch (const std::exception& error) {
      ++failed;
      failures.push_back(full + "\n    unexpected exception: " + error.what());
    } catch (...) {
      ++failed;
      failures.push_back(full + "\n    unexpected non-standard exception");
    }
  }
  std::cout << "checks: " << check_count().load() << "\n";
  std::cout << "tests passed: " << passed << ", failed: " << failed << "\n";
  for (const std::string& failure : failures) {
    std::cout << "FAILED " << failure << "\n";
  }
  std::cout.flush();
  return failed == 0 ? 0 : 1;
}

}  // namespace fls_test

#define FLS_CONCAT_DETAIL(a, b) a##b
#define FLS_CONCAT(a, b) FLS_CONCAT_DETAIL(a, b)

#define FLOW_TEST(suite_name, test_name)                                            \
  static void FLS_CONCAT(suite_name, FLS_CONCAT(_, FLS_CONCAT(test_name, _body)))(); \
  static const ::fls_test::Registrar FLS_CONCAT(                                    \
      suite_name, FLS_CONCAT(_, FLS_CONCAT(test_name, _registrar)))(                \
      #suite_name, #test_name,                                                      \
      &FLS_CONCAT(suite_name, FLS_CONCAT(_, FLS_CONCAT(test_name, _body))));        \
  static void FLS_CONCAT(suite_name, FLS_CONCAT(_, FLS_CONCAT(test_name, _body)))()

#define CHECK(condition)                                                              \
  ::fls_test::record_check(static_cast<bool>(condition), __FILE__, __LINE__,          \
                           std::string("CHECK failed: ") + #condition)

#define CHECK_EQ(actual, expected)                                                    \
  do {                                                                                \
    const auto fls_actual = (actual);                                                 \
    const auto fls_expected = (expected);                                             \
    ::fls_test::record_check(fls_actual == fls_expected, __FILE__, __LINE__,          \
                             std::string("CHECK_EQ failed: ") + #actual + " == " +    \
                                 #expected + " (actual=" + ::fls_test::show(fls_actual) + \
                                 ", expected=" + ::fls_test::show(fls_expected) + ")"); \
  } while (false)

#define CHECK_NE(actual, unexpected)                                                  \
  do {                                                                                \
    const auto fls_actual = (actual);                                                 \
    const auto fls_unexpected = (unexpected);                                         \
    ::fls_test::record_check(fls_actual != fls_unexpected, __FILE__, __LINE__,        \
                             std::string("CHECK_NE failed: ") + #actual + " != " +    \
                                 #unexpected);                                        \
  } while (false)

/// Evaluation order safe: the expression is evaluated exactly once. Accepts
/// both flow_scheduler::Status and flow_scheduler::Result<T>.
#define CHECK_OK(expr)                                                                \
  do {                                                                                \
    const auto fls_view = ::fls_test::status_view(expr);                              \
    ::fls_test::record_check(fls_view.ok, __FILE__, __LINE__,                         \
                             std::string("CHECK_OK failed: ") + #expr + " -> " +      \
                                 fls_view.text);                                      \
  } while (false)

#define CHECK_ERROR(expr, expected_code)                                              \
  do {                                                                                \
    const auto fls_value = (expr);                                                    \
    const auto fls_view = ::fls_test::status_view(fls_value);                         \
    ::fls_test::record_check(!fls_view.ok &&                                          \
                                 ::fls_test::status_code(fls_value) == (expected_code), \
                             __FILE__, __LINE__,                                      \
                             std::string("CHECK_ERROR failed: ") + #expr + " -> " +   \
                                 fls_view.text);                                      \
  } while (false)

/// A failed REQUIRE aborts the current test function; the runner records it.
#define REQUIRE(condition)                                                            \
  do {                                                                                \
    if (!static_cast<bool>(condition)) {                                              \
      ::fls_test::raise(__FILE__, __LINE__, std::string("REQUIRE failed: ") + #condition); \
    }                                                                                 \
  } while (false)

#define REQUIRE_EQ(actual, expected)                                                  \
  do {                                                                                \
    const auto fls_actual = (actual);                                                 \
    const auto fls_expected = (expected);                                             \
    if (!(fls_actual == fls_expected)) {                                              \
      ::fls_test::raise(__FILE__, __LINE__,                                           \
                        std::string("REQUIRE_EQ failed: ") + #actual + " == " +        \
                            #expected + " (actual=" + ::fls_test::show(fls_actual) +   \
                            ", expected=" + ::fls_test::show(fls_expected) + ")");     \
    }                                                                                 \
    ::fls_test::check_count().fetch_add(1, std::memory_order_relaxed);                \
  } while (false)

#define REQUIRE_OK(expr)                                                              \
  do {                                                                                \
    const auto fls_view = ::fls_test::status_view(expr);                              \
    if (!fls_view.ok) {                                                               \
      ::fls_test::raise(__FILE__, __LINE__,                                           \
                        std::string("REQUIRE_OK failed: ") + #expr + " -> " +         \
                            fls_view.text);                                           \
    }                                                                                 \
    ::fls_test::check_count().fetch_add(1, std::memory_order_relaxed);                \
  } while (false)

#endif  // FLOW_SCHEDULER_TEST_FRAMEWORK_HPP
