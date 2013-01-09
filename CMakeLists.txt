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

# verify path to proton-c has been configured
#
message ( "PROTON_SOURCE_DIR set to ${PROTON_SOURCE_DIR}" )
include_directories ("${PROTON_SOURCE_DIR}/include")
link_directories ("${PROTON_SOURCE_DIR}/build")
find_path( _PROTON_INCLUDE_PATH_
           NAMES messenger.h message.h
           PATHS "${PROTON_SOURCE_DIR}/include"
           PATH_SUFFIXES proton )
if (_PROTON_INCLUDE_PATH_ STREQUAL _PROTON_INCLUDE_PATH_-NOTFOUND)
   message( FATAL_ERROR "Proton not found: use 'cmake -DPROTON_SOURCE_DIR=' to set the location of the proton-c directory")
endif (_PROTON_INCLUDE_PATH_ STREQUAL _PROTON_INCLUDE_PATH_-NOTFOUND)
find_path( _PROTON_LIB_PATH_
           NAMES "libqpid-proton.so"
           PATHS "${PROTON_SOURCE_DIR}/build")
if (_PROTON_LIB_PATH_ STREQUAL _PROTON_LIB_PATH_-NOTFOUND)
   message( FATAL_ERROR "Proton library not found in ${PROTON_SOURCE_DIR}/build")
endif (_PROTON_LIB_PATH_ STREQUAL _PROTON_LIB_PATH_-NOTFOUND)

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
