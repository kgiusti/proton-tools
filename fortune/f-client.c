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
#include <getopt.h>
#include <uuid/uuid.h>



typedef struct {
    const char *address;
    const char *gateway_addr;
    const char *new_fortune;
    int timeout;  // milliseconds
    const char *reply_to;
    int send_bad_msg;
    unsigned int ttl;
    unsigned int retry;
} Options_t;

static void usage(int rc)
{
    printf("Usage: f-client [OPTIONS] <f-server>\n"
           "Get the current fortune message from <f-server>\n"
           " -a <f-server> \tThe address of the fortune server [amqp://0.0.0.0]\n"
           " -s <message> \tSet the server's fortune message to \"<message>\"\n"
           " -g <gateway> \tGateway to use to reach <f-server>\n"
           " -r <address> \tUse <address> for reply-to\n"
           " -t # \tTimeout in seconds [5]\n"
           " -l <secs> \tTTL to set in message, 0 = no TTL [0]\n"
           " -R # \tMessage send retry limit [3]\n"
           " -V \tEnable debug logging\n"
           " -X \tSend a bad message (forces a failure response from f-server\n"
           );
    exit(rc);
}

static void parse_options( int argc, char **argv, Options_t *opts )
{
    int c;
    opterr = 0;

    memset( opts, 0, sizeof(*opts) );
    opts->timeout = 5;
    opts->retry = 3;

    while ((c = getopt(argc, argv, "a:s:g:t:r:l:R:VX")) != -1) {
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
        case 'V': enable_logging(); break;
        case 'X': opts->send_bad_msg = 1; break;

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
    bool duplicate = false;

    rc = pn_data_scan( body, "{.S.S.S.S}",
                       &m_type, &m_command, &m_value, &m_status );
    check( rc == 0, "Failed to decode response message" );
    check(strncmp("response", m_type.start, m_type.size) == 0, "Unknown message type received");
    if (strncmp("DUPLICATE", m_status.start, m_status.size) == 0) {
        LOG( "Server detected duplicate request!\n" );
        duplicate = true;
    } else if (strncmp("OK", m_status.start, m_status.size)) {
        fprintf( stderr, "Request failed - error: %.*s\n", (int)m_status.size, m_status.start );
        return;
    }

    fprintf( stdout, "Fortune%s: \"%.*s\"%s\n",
             strncmp("set", m_command.start, m_command.size) == 0 ? " set to" : "",
             (int)m_value.size, m_value.start,
             duplicate ? " (duplicate detected by server)" : "" );
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

    // no need to track outstanding messages
    pn_messenger_set_outgoing_window( messenger, 0 );
    pn_messenger_set_incoming_window( messenger, 0 );

    pn_messenger_set_timeout( messenger, opts.timeout );

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
        reply_to = _strdup(opts.reply_to);
        check( reply_to, "Out of memory" );
#if 1
        // need to 'fix' the reply-to for use in the message itself:
        // no '~' is allowed in that case
        char *tilde = strstr( reply_to, "://~" );
        if (tilde) {
            tilde += 3;  // overwrite '~'
            memmove( tilde, tilde + 1, strlen( tilde + 1 ) + 1 );
        }
#endif
    }

    // Create a request message
    //
    const char *command = opts.new_fortune ? "set" : "get";
    build_request_message( request_msg,
                           opts.send_bad_msg ? "bad-command" : command,
                           opts.address, reply_to,
                           opts.new_fortune, opts.ttl );

    // set a unique identifier for this message, so remote can
    // de-duplicate when we re-transmit
    uuid_t uuid;
    uuid_generate(uuid);
    char uuid_str[37];
    uuid_unparse_upper(uuid, uuid_str);
    pn_data_put_string( pn_message_id( request_msg ),
                        pn_bytes( sizeof(uuid_str), uuid_str ));

    // set the correlation id so we can ensure the response matches
    // our request. (re-use uuid just 'cause it's easy!)
    pn_data_put_string( pn_message_correlation_id( request_msg ),
                        pn_bytes( sizeof(uuid_str), uuid_str ));

    int send_count = 0;
    bool done = false;

    // keep re-transmitting until something arrives
    do {
        LOG("sending request message...\n");
        rc = pn_messenger_put( messenger, request_msg );
        check(rc == 0, "pn_messenger_put() failed");
        send_count++;
        if (opts.retry) opts.retry--;

        LOG("waiting for response...\n");
        rc = pn_messenger_recv( messenger, -1 );
        if (rc == PN_TIMEOUT) {
            LOG( "Timed-out waiting for a response, retransmitting...\n" );
            pn_message_set_delivery_count( request_msg, send_count );
        } else {
            check(rc == 0, "pn_messenger_recv() failed\n");

            while (pn_messenger_incoming( messenger ) > 0) {
                rc = pn_messenger_get( messenger, response_msg );
                check(rc == 0, "pn_messenger_get() failed");

                LOG("response received!\n");
                // validate the correlation id
                pn_bytes_t cid = pn_data_get_string( pn_message_correlation_id( response_msg ) );
                if (cid.size == 0 || strncmp( uuid_str, cid.start, cid.size )) {
                    LOG( "Correlation Id mismatch!  Ignoring this response!\n" );
                } else {
                    process_reply( messenger, response_msg );
                    done = true;
                }
            }
        }
    } while (!done && opts.retry);

    if (!done) {
        fprintf( stderr, "Retries exhausted, no response received from server!\n" );
    }

    rc = pn_messenger_stop(messenger);
    check(rc == 0, "pn_messenger_stop() failed");

    pn_messenger_free(messenger);
    pn_message_free( response_msg );
    pn_message_free( request_msg );

    if (reply_to) free(reply_to);

    return 0;
}
