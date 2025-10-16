cmake_minimum_required(VERSION 3.16)
project(google-tests)

set(CMAKE_CXX_STANDARD 20)

add_subdirectory(lib)
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

# For GCC
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -Wno-error=stringop-overflow -fPIE -pie -march=native")

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "12.0")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --param destructive-interference-size=64")
    endif ()
endif ()

# For Clang
#set(CMAKE_CXX_COMPILER "clang++-18")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -fPIE")

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -O2")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -O2")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=shift -O2")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")

# set(CMAKE_CXX_FLAGS_DEBUG "-g")
# set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# When you want your debug mode program to go faster
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O2")

add_compile_definitions(ENJIMA_METRICS_LEVEL=2)
add_compile_definitions(ENJIMA_QUEUE_BASED_Q_TYPE=0)
add_compile_definitions(ENJIMA_MEM_CPY_METHOD=0)
add_compile_definitions(ENJIMA_BUFFER_ALIGNMENT=128)

add_executable(google_tests_run memory/MemoryManagerTest.cpp
        memory/MemoryChunkTest.cpp
        runtime/ExecutionEngineTest.cpp
        runtime/GenericOperatorTest.cpp
        runtime/OperatorProcessingValidityTest.cpp
        runtime/QueueOperatorTest.cpp
        operators/OperatorTypesTest.cpp
        common/HelperFunctions.h
        common/HelperTypes.h
        common/InputGenerationHelper.h
        common/TestSetupHelperFunctions.h
        metrics/MetricLoggingTest.cpp
        runtime/scheduling/PrioritySchedulingTest.cpp
        runtime/scheduling/RoundRobinSchedulingTest.cpp
        runtime/scheduling/ThreadBasedSchedulingTest.cpp
        runtime/MultiQueryTest.cpp
        runtime/FixedRateGenerationTest.cpp
        common/TestSetupHelperFunctions.cpp
        common/TestSetupHelperFunctions.tpp
)

target_link_libraries(google_tests_run Enjima gtest gtest_main)

add_custom_command(TARGET google_tests_run POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_SOURCE_DIR}/sample-configs/conf/enjima-config.yaml.sample
        ${CMAKE_CURRENT_BINARY_DIR}/conf/enjima-config.yaml)
