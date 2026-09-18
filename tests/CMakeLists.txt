# Flow Scheduler 1.0.0 test suite.
# Copyright 2026 Summon Software Labs.
# Licensed under the Apache License, Version 2.0.

set(FLOW_SCHEDULER_TEST_SOURCES
  test_main.cpp
  test_core.cpp
  test_arbitration.cpp
  test_fairness.cpp
  test_deadline.cpp
  test_preemption.cpp
  test_completion.cpp
  test_accounting.cpp
  test_persistence.cpp
  test_framing.cpp
  test_adversarial.cpp
  test_concurrency.cpp
  test_property.cpp
  test_bench.cpp
  test_multiprocess.cpp)

add_executable(flow_scheduler_tests ${FLOW_SCHEDULER_TEST_SOURCES})
target_link_libraries(flow_scheduler_tests PRIVATE flow_scheduler::flow_scheduler)
target_include_directories(flow_scheduler_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(flow_scheduler_tests PROPERTIES CXX_STANDARD 20 CXX_EXTENSIONS OFF)

# Paths used by the real multiprocess test. The tools are built into the same
# configuration directory as the test binary.
add_dependencies(flow_scheduler_tests flow_scheduler_coordinator flow_scheduler_worker)
target_compile_definitions(flow_scheduler_tests PRIVATE
  FLOW_SCHEDULER_COORDINATOR_PATH="$<TARGET_FILE:flow_scheduler_coordinator>"
  FLOW_SCHEDULER_WORKER_PATH="$<TARGET_FILE:flow_scheduler_worker>"
  FLOW_SCHEDULER_INSPECT_PATH="$<TARGET_FILE:flow_scheduler_inspect>")

if(MSVC)
  target_compile_options(flow_scheduler_tests PRIVATE
    /W4 /permissive- /utf-8 /Zc:__cplusplus /Zc:preprocessor /EHsc)
  target_compile_definitions(flow_scheduler_tests PRIVATE
    _CRT_SECURE_NO_WARNINGS NOMINMAX WIN32_LEAN_AND_MEAN)
  if(FLOW_SCHEDULER_WARNINGS_AS_ERRORS)
    target_compile_options(flow_scheduler_tests PRIVATE /WX)
  endif()
else()
  target_compile_options(flow_scheduler_tests PRIVATE -Wall -Wextra -Wpedantic)
  if(FLOW_SCHEDULER_WARNINGS_AS_ERRORS)
    target_compile_options(flow_scheduler_tests PRIVATE -Werror)
  endif()
endif()

if(FLOW_SCHEDULER_ENABLE_ASAN)
  if(MSVC)
    target_compile_options(flow_scheduler_tests PUBLIC /fsanitize=address)
    target_link_options(flow_scheduler_tests PUBLIC /INCREMENTAL:NO)
  else()
    target_compile_options(flow_scheduler_tests PUBLIC -fsanitize=address -fno-omit-frame-pointer -g)
    target_link_options(flow_scheduler_tests PUBLIC -fsanitize=address)
  endif()
endif()

# No test timeouts are configured anywhere: a test that hangs is a defect to
# diagnose, not something to paper over with a watchdog.
add_test(NAME flow_scheduler_tests COMMAND flow_scheduler_tests)
