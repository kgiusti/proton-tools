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

#include "common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <assert.h>

void msgr_die(const char *file, int line, const char *message)
{
  fprintf(stderr, "%s:%i: %s\n", file, line, message);
  exit(1);
}

//sigh - would be nice if proton exported pn_strdup()
char *msgr_strdup( const char *src )
{
  char *r = NULL;
  if (src) {
    r = (char *) malloc(sizeof(char) * (strlen(src) + 1));
    if (r) strcpy(r,src);
  }
  return r;
}

//sigh part deux - would be nice if proton exported pn_i_now()
pn_timestamp_t msgr_now()
{
  struct timeval now;
  if (gettimeofday(&now, NULL)) abort();
  return ((pn_timestamp_t)now.tv_sec) * 1000 + (now.tv_usec / 1000);
}

