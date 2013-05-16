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
#include "proton/messenger.h"

void enable_logging();
void LOG( const char *fmt, ... );

void DIE( const char *file, int line, const char *fmt, ... );
char *_strdup( const char *src );
pn_timestamp_t _now();

#define check( expression, message )  \
  { if (!(expression)) DIE(__FILE__,__LINE__, message); }

#define check_messenger(m)                                      \
  { check(pn_messenger_errno(m) == 0, pn_messenger_error(m)) }

pn_status_t deliver_message( pn_messenger_t *messenger,
                             pn_message_t *message );

typedef struct DeduplicationDb_s DeduplicationDb_t;
typedef void DeduplicationDeleter_t( void *handle, const char *key, void *data );

DeduplicationDb_t *DeduplicationDbNew( DeduplicationDeleter_t *, void *handle );
void DeduplicationDbDelete( DeduplicationDb_t * );

void DeduplicationRemember( DeduplicationDb_t *,
                            const char *key,
                            void *data,
                            pn_timestamp_t expire );

void DeduplicationForget( DeduplicationDb_t *, const char *key );

bool DeduplicationIsDuplicate( DeduplicationDb_t *, const char *key,
                               void **data );

pn_timestamp_t DeduplicationPurgeExpired( DeduplicationDb_t * );


