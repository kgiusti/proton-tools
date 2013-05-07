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



typedef struct {
    const char *address;
    const char *gateway_addr;
    const char *new_message;
    int timeout;  // seconds
    const char *reply_to;
    int reject;
} Options_t;

static int log = 0;
#define LOG(...)                                        \
    if (log) { fprintf( stdout, __VA_ARGS__ ); }

static void usage(int rc)
{
    printf("Usage: f-client [OPTIONS] <f-server>\n"
           "Get the current fortune message from <f-server>\n"
           " -a <f-server> \tThe address of the fortune server [amqp://0.0.0.0]\n"
           " -s <message> \tSet the server's fortune message to \"<message>\"\n"
           " -g <gateway> \tGateway to use to reach <f-server>\n"
           " -r <address> \tUse <address> for reply-to\n"
           " -t # \tInactivity timeout in seconds, -1 = no timeout [-1]\n"
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
    opts->timeout = -1;

    while ((c = getopt(argc, argv, "a:s:g:t:r:VX")) != -1) {
        switch (c) {
        case 'a': opts->address = optarg; break;
        case 's': opts->new_message = optarg; break;
        case 'g': opts->gateway_addr = optarg; break;
        case 't':
            if (sscanf( optarg, "%d", &opts->timeout ) != 1) {
                fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
                usage(1);
            }
            if (opts->timeout > 0) opts->timeout *= 1000;
            break;
        case 'r': opts->reply_to = optarg; break;
        case 'V': log = 1; break;
        case 'X': opts->reject = 1; break;

        default:
            usage(1);
        }
    }

    if (!opts->address) opts->address = "amqp://0.0.0.0";
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


/* set request message format:
   { "type": "request",
     "command": "set",
     "value": <new fortune string>,
   }
*/
static pn_message_t *build_set_message( const char *new_message )
{
    int rc;

    pn_message_t *message = pn_message();
    if (!message) return message;

    pn_data_t *body = pn_message_body(message);
    pn_data_clear( body );
    rc = pn_data_fill( body, "{SSSSSS}",
                       "type", "request",
                       "command", "set",
                       "value", new_message );
    check( rc == 0, "Failure to create set message" );
    return message;
}


/* get request message format:
   { "type": "request",
     "command": "set",
     "value":"",
   }
*/
static pn_message_t *build_get_message( void )
{
    int rc;

    pn_message_t *message = pn_message();
    if (!message) return message;

    pn_data_t *body = pn_message_body(message);
    pn_data_clear( body );
    rc = pn_data_fill( body, "{SSSSSS}",
                       "type", "request",
                       "command", "get",
                       "value", "" );
    check( rc == 0, "Failure to create get message" );
    return message;
}


int main(int argc, char** argv)
{
    Options_t opts;
    int rc;

    pn_message_t *message = 0;
    pn_messenger_t *messenger = 0;

    parse_options( argc, argv, &opts );

    messenger = pn_messenger( 0 );
    check( messenger, "Failed to allocate a Messenger");

    // only 1 message (request/response) outstanding at a time
    pn_messenger_set_outgoing_window( messenger, 1 );
    pn_messenger_set_incoming_window( messenger, 1 );
    pn_messenger_set_timeout( messenger, opts.timeout );
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

    if (opts.reject) {
        // empty test message that forces a reject from the server
        message = pn_message();
    } else if (opts.new_message) {
        message = build_set_message( opts.new_message );
    } else {
        message = build_get_message( );
    }
    check(message, "failed to allocate a message");

    pn_atom_t id;
    id.type = PN_ULONG;
    id.u.as_ulong = 0;
    pn_message_set_correlation_id( message, id );
    //pn_message_set_creation_time( message, msgr_now() );
    pn_message_set_address( message, opts.address );
    if (reply_to) {
        LOG("setting reply-to %s\n", reply_to);
        rc = pn_message_set_reply_to( message, reply_to );
        check(rc == 0, "pn_message_set_reply_to() failed");
    }
    pn_messenger_put(messenger, message);
    pn_tracker_t request_tracker = pn_messenger_outgoing_tracker(messenger);
    // TODO: how can I tell if this succeeded???

    LOG("sending request...\n");
    rc = pn_messenger_send(messenger);
    check(rc == 0, "pn_messenger_send() failed");

    pn_status_t status = pn_messenger_status( messenger, request_tracker );
    switch (status) {
    case PN_STATUS_ACCEPTED:
        LOG("Request message accepted by remote.\n");
        break;
    case PN_STATUS_REJECTED:
        fprintf(stderr, "Request rejected by remote, exiting\n");
        check( false, "done!");
    default:
        // TODO: retry on failure
        fprintf(stderr, "Unexpected request status: %d\n", (int) status);
        check( false, "done!" );
    }

    rc = pn_messenger_settle( messenger, request_tracker, 0 );
    check(rc == 0, "pn_messenger_settle() failed");

    LOG("waiting for response...\n");
    rc = pn_messenger_recv(messenger, 1);
    check(rc == 0, "pn_messenger_recv() failed");   // TODO: deal with timeout
    rc = pn_messenger_get(messenger, message);
    check(rc == 0, "pn_messenger_get() failed");
    LOG("response received!\n");
    pn_tracker_t response_tracker = pn_messenger_incoming_tracker(messenger);
    rc = pn_messenger_accept( messenger, response_tracker, 0 );
    check(rc == 0, "pn_messenger_accept() failed");
    rc = pn_messenger_settle( messenger, response_tracker, 0 );
    check(rc == 0, "pn_messenger_settle() failed");
    LOG("response accepted!\n");

    process_reply( messenger, message);

    rc = pn_messenger_stop(messenger);
    check(rc == 0, "pn_messenger_stop() failed");
    check_messenger(messenger);

    pn_messenger_free(messenger);
    pn_message_free(message);

    if (reply_to) free(reply_to);

    return 0;
}
