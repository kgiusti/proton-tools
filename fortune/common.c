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
#include <unistd.h>
#include <assert.h>

static int log = 0;

void enable_logging()
{
    log = 1;
}

void LOG( const char *fmt, ... )
{
    if (log) {
        va_list ap;
        va_start(ap, fmt);
        vfprintf( stdout, fmt, ap );
        va_end(ap);
    }
}

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


////////////////////////////////////////////////////////////////////////////////
// Send a message and confirm receipt by remote.
//
// retries - re-send up to this many times
// timeout_msecs - wait at least this long for confirmation by remote
// backoff_secs - wait at least this long between re-sends
//
DeliveryStatus_t deliver_message( pn_messenger_t *messenger,
                                  pn_message_t *message,
                                  unsigned int retries,
                                  int timeout_msecs,
                                  int backoff_secs)
{
    DeliveryStatus_t result = STATUS_FAILED;
    int old_timeout = pn_messenger_get_timeout( messenger );
    pn_messenger_set_timeout( messenger, timeout_msecs );
    bool done = false;
    while (!done && retries) {
        pn_messenger_put( messenger, message );
        pn_tracker_t request_tracker = pn_messenger_outgoing_tracker(messenger);

        LOG("sending message...\n");
        int rc = pn_messenger_send( messenger );
        if (rc) fprintf(stderr, "pn_messenger_send() failed: error=%d\n", rc );

        pn_status_t status = pn_messenger_status( messenger, request_tracker );
        if (status == PN_STATUS_REJECTED) {
            fprintf(stderr, "Sent message rejected by remote!\n");
            result = STATUS_REJECTED;
            LOG("settling locally...\n");
            rc = pn_messenger_settle( messenger, request_tracker, 0 );
            check(rc == 0, "pn_messenger_settle() failed");
            done = true;
        } else if (status == PN_STATUS_ACCEPTED) {
            LOG("Sent message accepted by remote.\n");
            result = STATUS_ACCEPTED;
            LOG("settling locally...\n");
            rc = pn_messenger_settle( messenger, request_tracker, 0 );
            check(rc == 0, "pn_messenger_settle() failed");
            done = true;
        } else {
            fprintf(stderr, "Unexpected status from peer: %d\n", (int) status);
            LOG("settling locally...\n");
            rc = pn_messenger_settle( messenger, request_tracker, 0 );
            check(rc == 0, "pn_messenger_settle() failed");

            pn_message_set_delivery_count( message,
                                           pn_message_get_delivery_count( message ) + 1 );

            if (backoff_secs) sleep( backoff_secs );
            --retries;
            if (retries) LOG("Resending...\n");
        }
    }

    // BEGIN HACK: at this point, the fact that we called "settle"
    // hasn't been communicated to the peer.  The peer (receiver) will
    // not settle its tracker until it "hears" that the sender has
    // settled first (see the AMQP 1.0 spec - exactly once delivery).
    // The following hack attempts to do this.  Is it necessary?  Will
    // pn_messenger_stop() send the settle should we never call into
    // pn_messenger_send/recv() again?
    //
    if (result != STATUS_FAILED) {
        pn_messenger_set_timeout( messenger, 0 );
        pn_messenger_recv( messenger, -1 );
    }
    // ? How can I confirm the remote settled (already have outcome) ?
    // end HACK

    pn_messenger_set_timeout( messenger, old_timeout );
    return result;
}


