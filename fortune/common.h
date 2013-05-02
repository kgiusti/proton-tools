/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "proton/types.h"
#include "proton/message.h"

void msgr_die(const char *file, int line, const char *message);
char *msgr_strdup( const char *src );
pn_timestamp_t msgr_now();
void parse_password( const char *, char ** );

#define check_messenger(m)  \
  { check(pn_messenger_errno(m) == 0, pn_messenger_error(m)) }

#define check( expression, message )  \
  { if (!(expression)) msgr_die(__FILE__,__LINE__, message); }


#if defined(_WIN32) && ! defined(__CYGWIN__)
#include "../wincompat/getopt.h"
#else
#include <getopt.h>
#endif

