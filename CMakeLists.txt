#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

cmake_minimum_required (VERSION 2.6)

find_library(PROTON_LIB qpid-proton
             PATH "${PROTON_SOURCE_DIR}/build")
find_path(PROTON_INCLUDE proton/driver.h
          PATH "${PROTON_SOURCE_DIR}/include")

message( "PROTON_LIB set to ${PROTON_LIB}" )
message( "PROTON_INCLUDE set to ${PROTON_INCLUDE}" )

# verify path to proton-c has been configured
#
if (PROTON_LIB STREQUAL PROTON_LIB-NOTFOUND)
   message( FATAL_ERROR "Proton libraries not found: use 'cmake -DPROTON_SOURCE_DIR=' to set the location of the proton-c directory")
endif (PROTON_LIB STREQUAL PROTON_LIB-NOTFOUND)

if (PROTON_INCLUDE STREQUAL PROTON_INCLUDE-NOTFOUND)
   message( FATAL_ERROR "Proton include files not found: 'cmake -DPROTON_SOURCE_DIR=' to set the location of the proton-c directory")
endif (PROTON_INCLUDE STREQUAL PROTON_INCLUDE-NOTFOUND)

if (CMAKE_COMPILER_IS_GNUCC)
  if (ENABLE_WARNING_ERROR)
    set (WERROR "-Werror")
  endif (ENABLE_WARNING_ERROR)
  set (COMPILE_WARNING_FLAGS "${WERROR} -Wall -pedantic-errors")
  set (COMPILE_LANGUAGE_FLAGS "-std=c99")
endif (CMAKE_COMPILER_IS_GNUCC)

set (CMAKE_BUILD_TYPE RelWithDebInfo)

add_executable(perf-recv perf-recv.c)
add_executable(perf-send perf-send.c)

target_link_libraries(perf-recv qpid-proton)
target_link_libraries(perf-send qpid-proton)

set_target_properties (
  perf-recv perf-send
  PROPERTIES
  COMPILE_FLAGS "${COMPILE_WARNING_FLAGS} ${COMPILE_LANGUAGE_FLAGS}"
)
