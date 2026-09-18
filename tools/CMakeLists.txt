# Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
# Copyright 2026 Summon Software Labs.
# Licensed under the Apache License, Version 2.0.

# ---------------------------------------------------------------------------
# Warning configuration, identical to the other first-party targets
# ---------------------------------------------------------------------------
function(flow_scheduler_apply_warning_flags target)
  if(MSVC)
    target_compile_options(${target} PRIVATE
      /W4 /permissive- /utf-8 /Zc:__cplusplus /Zc:preprocessor /EHsc)
    if(FLOW_SCHEDULER_WARNINGS_AS_ERRORS)
      target_compile_options(${target} PRIVATE /WX)
    endif()
    target_compile_definitions(${target} PRIVATE
      _CRT_SECURE_NO_WARNINGS NOMINMAX WIN32_LEAN_AND_MEAN)
  else()
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
    if(FLOW_SCHEDULER_WARNINGS_AS_ERRORS)
      target_compile_options(${target} PRIVATE -Werror)
    endif()
  endif()
  if(FLOW_SCHEDULER_ENABLE_ASAN)
    if(MSVC)
      target_compile_options(${target} PUBLIC /fsanitize=address)
      target_link_options(${target} PUBLIC /INCREMENTAL:NO)
    else()
      target_compile_options(${target} PUBLIC -fsanitize=address -fno-omit-frame-pointer -g)
      target_link_options(${target} PUBLIC -fsanitize=address)
    endif()
  endif()
endfunction()

function(flow_scheduler_configure_tool target)
  target_link_libraries(${target} PRIVATE flow_scheduler::flow_scheduler)
  set_target_properties(${target} PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF)
  flow_scheduler_apply_warning_flags(${target})
endfunction()

# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
add_executable(flow_scheduler_coordinator fs_coordinator.cpp)
add_executable(flow_scheduler_worker fs_worker.cpp)
add_executable(flow_scheduler_inspect fs_inspect.cpp)

flow_scheduler_configure_tool(flow_scheduler_coordinator)
flow_scheduler_configure_tool(flow_scheduler_worker)
flow_scheduler_configure_tool(flow_scheduler_inspect)

install(TARGETS
  flow_scheduler_coordinator
  flow_scheduler_worker
  flow_scheduler_inspect
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
