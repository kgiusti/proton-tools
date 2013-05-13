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
#include "proton/message.h"
#include "proton/messenger.h"
#include "proton/error.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <uuid/uuid.h>



typedef struct {
    const char *address;
    const char *gateway_addr;
    const char *new_fortune;
    int timeout;  // seconds
    const char *reply_to;
    int reject;
    unsigned int ttl;
    unsigned int retry;
    unsigned int backoff;
} Options_t;

static void usage(int rc)
{
    printf("Usage: f-client [OPTIONS] <f-server>\n"
           "Get the current fortune message from <f-server>\n"
           " -a <f-server> \tThe address of the fortune server [amqp://0.0.0.0]\n"
           " -s <message> \tSet the server's fortune message to \"<message>\"\n"
           " -g <gateway> \tGateway to use to reach <f-server>\n"
           " -r <address> \tUse <address> for reply-to\n"
           " -t # \tTimeout in seconds [1]\n"
           " -l <secs> \tTTL to set in message, 0 = no TTL [0]\n"
           " -R # \tMessage send retry limit [10]\n"
           " -B <secs> \tRetry backoff timeout [5]\n"
           " -V \tEnable debug logging\n"
           " -X \tSend a bad message (forces a REJECT from f-server\n"
           );
    exit(rc);
}

static void parse_options( int argc, char **argv, Options_t *opts )
{
    int c;
    opterr = 0;

    memset( opts, 0, sizeof(*opts) );
    opts->timeout = 1;
    opts->retry = 3;
    opts->backoff = 5;

    while ((c = getopt(argc, argv, "a:s:g:t:r:l:R:B:VX")) != -1) {
        switch (c) {
        case 'a': opts->address = optarg; break;
        case 's': opts->new_fortune = optarg; break;
        case 'g': opts->gateway_addr = optarg; break;
        case 't':
            if (sscanf( optarg, "%d", &opts->timeout ) != 1) {
                fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
                usage(1);
            }
            break;
        case 'r': opts->reply_to = optarg; break;
        case 'l':
            if (sscanf( optarg, "%u", &opts->ttl ) != 1) {
                fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
                usage(1);
            }
            break;
        case 'R':
            if (sscanf( optarg, "%u", &opts->retry ) != 1) {
                fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
                usage(1);
            }
            break;
        case 'B':
            if (sscanf( optarg, "%u", &opts->backoff ) != 1) {
                fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
                usage(1);
            }
            break;
        case 'V': enable_logging(); break;
        case 'X': opts->reject = 1; break;

        default:
            usage(1);
        }
    }

    if (!opts->address) opts->address = "amqp://0.0.0.0";
    if (opts->timeout > 0) opts->timeout *= 1000;
}


static void process_reply( pn_messenger_t *messenger,
                           pn_message_t *message)
{
    int rc;
    pn_data_t *body = pn_message_body(message);
    pn_bytes_t m_type;
    pn_bytes_t m_command;
    pn_bytes_t m_value;
    pn_bytes_t m_status;
    rc = pn_data_scan( body, "{.S.S.S.S}",
                       &m_type, &m_command, &m_value, &m_status );
    check( rc == 0, "Failed to decode response message" );
    check(strncmp("response", m_type.start, m_type.size) == 0, "Unknown message type received");
    if (strncmp("OK", m_status.start, m_status.size)) {
        fprintf( stderr, "Request failed - error: %.*s\n", (int)m_status.size, m_status.start );
        return;
    }
    if (strncmp("get", m_command.start, m_command.size) == 0) {
        fprintf( stdout, "Fortune: \"%.*s\"\n", (int)m_value.size, m_value.start );
    } else {
        fprintf( stdout, "Fortune set to \"%.*s\"\n", (int)m_value.size, m_value.start );
    }
}



static pn_message_t *build_request_message(pn_message_t *message,
                                           const char *command,
                                           const char *to,
                                           const char *reply_to,
                                           const char *new_fortune,
                                           unsigned int ttl)
{
    int rc;

    pn_message_set_address( message, to );
    if (reply_to) {
        LOG("setting reply-to %s\n", reply_to);
        rc = pn_message_set_reply_to( message, reply_to );
        check(rc == 0, "pn_message_set_reply_to() failed");
    }
    pn_message_set_delivery_count( message, 0 );
    if (ttl)
        pn_message_set_ttl( message, ttl * 1000 );

    // unique identifier for this message
    uuid_t uuid;
    uuid_generate(uuid);
    pn_atom_t id;
    id.type = PN_UUID;
    memcpy( id.u.as_uuid.bytes, uuid, sizeof(id.u.as_uuid.bytes) );
    pn_message_set_id( message, id );

    pn_data_t *body = pn_message_body(message);
    pn_data_clear( body );
    rc = pn_data_fill( body, "{SSSSSS}",
                       "type", "request",
                       "command", command,
                       "value", (new_fortune) ? new_fortune : "" );
    check( rc == 0, "Failure to create request message" );
    return message;
}


int main(int argc, char** argv)
{
    Options_t opts;
    int rc;

    pn_message_t *response_msg = pn_message();
    check( response_msg, "Failed to allocate a Message");
    pn_message_t *request_msg = pn_message();
    check( request_msg, "Failed to allocate a Message");
    pn_messenger_t *messenger = pn_messenger( 0 );
    check( messenger, "Failed to allocate a Messenger");

    parse_options( argc, argv, &opts );

    // only 1 message (request/response) outstanding at a time
    pn_messenger_set_outgoing_window( messenger, 1 );
    pn_messenger_set_incoming_window( messenger, 1 );

    if (opts.gateway_addr) {
        LOG( "routing all messages via %s\n", opts.gateway_addr );
        rc = pn_messenger_route( messenger,
                                 "*", opts.gateway_addr );
        check( rc == 0, "pn_messenger_route() failed" );
    }

    pn_messenger_start(messenger);

    char *reply_to = NULL;
    if (opts.reply_to) {
        LOG("subscribing to %s for replies\n", opts.reply_to);
        pn_messenger_subscribe(messenger, opts.reply_to);
        reply_to = msgr_strdup(opts.reply_to);
        check( reply_to, "Out of memory" );
        // need to 'fix' the reply-to for use in the message itself:
        // no '~' is allowed in that case
        char *tilde = strstr( reply_to, "://~" );
        if (tilde) {
            tilde += 3;  // overwrite '~'
            memmove( tilde, tilde + 1, strlen( tilde + 1 ) + 1 );
        }
    }

    // Create a request message
    //
    const char *command = opts.new_fortune ? "set" : "get";
    build_request_message( request_msg,
                           opts.reject ? "bad-command" : command,
                           opts.address, reply_to,
                           opts.new_fortune, opts.ttl );

    DeliveryStatus_t ds = deliver_message( messenger, request_msg,
                                           opts.retry, opts.timeout, opts.backoff );
    if (ds == STATUS_REJECTED) {
        fprintf( stderr, "Remote rejected the request - exiting." );
        exit(1);
    } else if (ds != STATUS_ACCEPTED) {
        LOG("Remote may not have received the request!  Waiting for reply anyways...\n");
    }

    //
    // Wait for the response
    //
    pn_messenger_set_timeout( messenger, opts.timeout );
    LOG("waiting for response...\n");

    rc = pn_messenger_recv( messenger, 1 );
    if (rc) LOG("pn_messenger_recv() failed: error=%d\n", rc );
    check(pn_messenger_incoming( messenger ), "No response received from server!\n");

    rc = pn_messenger_get( messenger, response_msg );
    check(rc == 0, "pn_messenger_get() failed");
    LOG("response received!\n");

    process_reply( messenger, response_msg );

    // Now tell the server we've moved to our terminal state (ACCEPT)
    pn_tracker_t response_tracker = pn_messenger_incoming_tracker( messenger );
    rc = pn_messenger_accept( messenger, response_tracker, 0 );
    check(rc == 0, "pn_messenger_accept() failed");

    // BEGIN HACK: wait until remote has seen our outcome state and
    // has settled its delivery
    pn_messenger_set_timeout( messenger, 500 );
    pn_messenger_recv( messenger, -1 );
    if (pn_messenger_status( messenger, response_tracker) == PN_STATUS_UNKNOWN) {
        fprintf( stderr, "Timed out waiting for server to settle the response.\n" );
    }
    pn_messenger_set_timeout( messenger, opts.timeout );
    // END HACK

    LOG("settling the received response...\n");
    rc = pn_messenger_settle( messenger, response_tracker, 0 );
    check(rc == 0, "pn_messenger_settle() failed");
    LOG("settled response delivery!\n");

    rc = pn_messenger_stop(messenger);
    check(rc == 0, "pn_messenger_stop() failed");
    check_messenger(messenger);

    pn_messenger_free(messenger);
    pn_message_free( response_msg );
    pn_message_free( request_msg );

    if (reply_to) free(reply_to);

    return 0;
}
