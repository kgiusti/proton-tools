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

# not sure of this minimum, could be less - all I've tested with so far
cmake_minimum_required (VERSION 2.6)

include(FindPkgConfig)

pkg_search_module( GLIB2 REQUIRED "glib-2.0" )
pkg_search_module( UUID REQUIRED "uuid" )

find_library(PROTON_LIB qpid-proton
             PATH "${PROTON_SOURCE_DIR}/build/proton-c")
find_path(PROTON_INCLUDE proton/driver.h
          PATH "${PROTON_SOURCE_DIR}/proton-c/include")

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

include_directories( ${CMAKE_CURRENT_SOURCE_DIR}/include
                     ${PROTON_INCLUDE}
                     ${GLIB2_INCLUDE_DIRS}
                     ${UUID_INCLUDE_DIRS}
)

if (CMAKE_COMPILER_IS_GNUCC)
  if (ENABLE_WARNING_ERROR)
    set (WERROR "-Werror")
  endif (ENABLE_WARNING_ERROR)
  set (COMPILE_WARNING_FLAGS "${WERROR} -Wall -pedantic-errors -Wno-comment")
  set (COMPILE_LANGUAGE_FLAGS "-std=c99")
endif (CMAKE_COMPILER_IS_GNUCC)

set (CMAKE_BUILD_TYPE RelWithDebInfo)

add_subdirectory(lib)
add_subdirectory(fortune)
add_subdirectory( banco-de-justin )
#add_subdirectory(perf)
