cmake_minimum_required(VERSION 3.20)

project(FlowScheduler
  VERSION 1.0.0
  DESCRIPTION "Deterministic temporal arbitration runtime for competing network flows"
  HOMEPAGE_URL "https://github.com/summonlabs/Flow-Scheduler"
  LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
option(FLOW_SCHEDULER_BUILD_TESTS    "Build the Flow Scheduler test suite"         ON)
option(FLOW_SCHEDULER_BUILD_TOOLS    "Build the Flow Scheduler command line tools" ON)
option(FLOW_SCHEDULER_BUILD_BENCH    "Build the synthetic benchmark harness"       ON)
option(FLOW_SCHEDULER_WARNINGS_AS_ERRORS "Treat first-party compiler warnings as errors" ON)
option(FLOW_SCHEDULER_ENABLE_ASAN    "Build with AddressSanitizer when supported"  OFF)
option(FLOW_SCHEDULER_BUILD_SHARED   "Build flow_scheduler as a shared library"    OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
endif()

if(FLOW_SCHEDULER_BUILD_SHARED)
  set(FLOW_SCHEDULER_LIBRARY_TYPE SHARED)
else()
  set(FLOW_SCHEDULER_LIBRARY_TYPE STATIC)
endif()

# ---------------------------------------------------------------------------
# Target
# ---------------------------------------------------------------------------
add_library(flow_scheduler ${FLOW_SCHEDULER_LIBRARY_TYPE})
add_library(flow_scheduler::flow_scheduler ALIAS flow_scheduler)

target_sources(flow_scheduler PRIVATE
  src/accounting.cpp
  src/checked.cpp
  src/dispatch.cpp
  src/hash.cpp
  src/error.cpp
  src/explain.cpp
  src/identity.cpp
  src/journal.cpp
  src/model.cpp
  src/schedule.cpp
  src/scheduler.cpp
  src/time.cpp
  src/framing.cpp
  src/transport.cpp
  src/protocol.cpp
  src/bench.cpp
  src/version.cpp)

target_compile_features(flow_scheduler PUBLIC cxx_std_20)

target_include_directories(flow_scheduler
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src)

set_target_properties(flow_scheduler PROPERTIES
  VERSION ${PROJECT_VERSION}
  SOVERSION ${PROJECT_VERSION_MAJOR}
  POSITION_INDEPENDENT_CODE ON
  CXX_EXTENSIONS OFF
  CXX_STANDARD 20
  CXX_STANDARD_REQUIRED ON
  EXPORT_NAME flow_scheduler)

# ---------------------------------------------------------------------------
# Warnings
# ---------------------------------------------------------------------------
if(MSVC)
  target_compile_options(flow_scheduler PRIVATE
    /W4 /permissive- /utf-8 /Zc:__cplusplus /Zc:preprocessor /EHsc)
  if(FLOW_SCHEDULER_WARNINGS_AS_ERRORS)
    target_compile_options(flow_scheduler PRIVATE /WX)
  endif()
  target_compile_definitions(flow_scheduler PRIVATE
    _CRT_SECURE_NO_WARNINGS NOMINMAX WIN32_LEAN_AND_MEAN)
  target_link_libraries(flow_scheduler PUBLIC ws2_32)
else()
  target_compile_options(flow_scheduler PRIVATE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
    -Wold-style-cast -Wcast-align -Wunused -Woverloaded-virtual
    -Wnull-dereference -Wdouble-promotion -Wformat=2)
  if(FLOW_SCHEDULER_WARNINGS_AS_ERRORS)
    target_compile_options(flow_scheduler PRIVATE -Werror)
  endif()
  find_package(Threads REQUIRED)
  target_link_libraries(flow_scheduler PUBLIC Threads::Threads)
endif()

if(FLOW_SCHEDULER_ENABLE_ASAN)
  if(MSVC)
    target_compile_options(flow_scheduler PUBLIC /fsanitize=address)
    target_link_options(flow_scheduler PUBLIC /INCREMENTAL:NO)
  else()
    target_compile_options(flow_scheduler PUBLIC -fsanitize=address -fno-omit-frame-pointer -g)
    target_link_options(flow_scheduler PUBLIC -fsanitize=address)
  endif()
endif()

# ---------------------------------------------------------------------------
# Install / export
# ---------------------------------------------------------------------------
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS flow_scheduler
  EXPORT FlowSchedulerTargets
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT FlowSchedulerTargets
  FILE FlowSchedulerTargets.cmake
  NAMESPACE flow_scheduler::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/FlowScheduler)

configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake/FlowSchedulerConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/FlowSchedulerConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/FlowScheduler)

write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/FlowSchedulerConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
  ARCH_INDEPENDENT)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/FlowSchedulerConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/FlowSchedulerConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/FlowScheduler)

install(FILES LICENSE README.md
  DESTINATION ${CMAKE_INSTALL_DATADIR}/doc/FlowScheduler
  OPTIONAL)

# ---------------------------------------------------------------------------
# Tools, benchmarks, tests
# ---------------------------------------------------------------------------
if(FLOW_SCHEDULER_BUILD_TOOLS)
  add_subdirectory(tools)
endif()

if(FLOW_SCHEDULER_BUILD_BENCH)
  add_executable(flow_scheduler_bench bench/flow_scheduler_bench.cpp)
  target_link_libraries(flow_scheduler_bench PRIVATE flow_scheduler::flow_scheduler)
  set_target_properties(flow_scheduler_bench PROPERTIES CXX_STANDARD 20 CXX_EXTENSIONS OFF)
  if(MSVC)
    target_compile_options(flow_scheduler_bench PRIVATE /W4 /permissive- /utf-8 /Zc:__cplusplus /EHsc)
    if(FLOW_SCHEDULER_WARNINGS_AS_ERRORS)
      target_compile_options(flow_scheduler_bench PRIVATE /WX)
    endif()
  else()
    target_compile_options(flow_scheduler_bench PRIVATE -Wall -Wextra -Wpedantic)
    if(FLOW_SCHEDULER_WARNINGS_AS_ERRORS)
      target_compile_options(flow_scheduler_bench PRIVATE -Werror)
    endif()
  endif()
  install(TARGETS flow_scheduler_bench RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()

if(FLOW_SCHEDULER_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
