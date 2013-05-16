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
#include <unistd.h>
#include <getopt.h>
#include <uuid/uuid.h>

typedef struct {
    const char *address;
    const char *gateway_addr;
    unsigned int delay;       // seconds
    unsigned int dup_timeout; // for duplication detection (seconds)
} Options_t;

static char *fortune;

typedef enum {
    SET_COMMAND,
    GET_COMMAND
} command_t;

static void usage(int rc)
{
    printf("Usage: f-server [OPTIONS] \n"
           " -a <addr> \tAddress to listen on [amqp://~0.0.0.0]\n"
           " -g <gateway> \tGateway for sending all reply messages\n"
           " -d <seconds> \tSimulate delay by sleeping <seconds> before replying [0]\n"
           " -l <seconds> \tDefault lifetime for detecting duplicates [60]\n"
           " -V \tEnable debug logging\n"
           );
    exit(rc);
}

static void parse_options( int argc, char **argv, Options_t *opts )
{
    int c;
    opterr = 0;

    memset( opts, 0, sizeof(*opts) );
    opts->dup_timeout = 60;

    while ((c = getopt(argc, argv, "a:g:d:V")) != -1) {
        switch (c) {
        case 'a': opts->address = optarg; break;
        case 'g': opts->gateway_addr = optarg; break;
        case 'd':
            if (sscanf( optarg, "%u", &opts->delay ) != 1) {
                fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
                usage(1);
            }
            break;
        case 'l':
            if (sscanf( optarg, "%u", &opts->dup_timeout ) != 1) {
                fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
                usage(1);
            }
            break;
        case 'V': enable_logging(); break;

        default:
            usage(1);
        }
    }

    if (!opts->address) opts->address = "amqp://~0.0.0.0";
}


/* Reply message format:
   { "type": "response",
     "command": ["get" | "set"],
     "value": <fortune string>,
     "status": ["OK"|<error string>]
   }
*/
static void build_response_message( pn_message_t *message,
                                    const char *reply_to,
                                    command_t command,
                                    const char *status,
                                    const char *value )
{
    pn_message_set_address( message, reply_to );
    pn_message_set_creation_time( message, _now() );
    pn_message_set_delivery_count( message, 0 );

    pn_data_t *body = pn_message_body(message);
    pn_data_clear( body );
    int rc = pn_data_fill( body, "{SSSSSSSS}",
                           "type", "response",
                           "command", command == SET_COMMAND ? "set" : "get",
                           "value", value ? value : "",
                           "status", status );
    check( rc == 0, "Failure to create response message" );
}


// decode the request message.
// returns 0 on success, else error
static int decode_request( pn_message_t *message,
                           command_t *command,
                           char **new_fortune )
{
    int rc;
    pn_data_t *body = pn_message_body(message);
    pn_bytes_t m_type;
    pn_bytes_t m_command;
    pn_bytes_t m_value;

    *new_fortune = NULL;

    rc = pn_data_scan( body, "{.S.S.S}",
                       &m_type, &m_command, &m_value );
    if (rc) {
        LOG( "Failed to decode request message" );
        return -1;
    }

    if (strncmp("request", m_type.start, m_type.size)) {
        LOG("Unknown message type received: %.*s\n", (int)m_type.size, m_type.start );
        return -1;
    }

    if (strncmp("get", m_command.start, m_command.size) == 0) {
        LOG("Received GET request\n");
        *command = GET_COMMAND;
    } else if (strncmp("set", m_command.start, m_command.size) == 0) {
        *command = SET_COMMAND;
        *new_fortune = (char *) malloc(sizeof(char) * (m_value.size + 1));
        check( *new_fortune, "Out of memory" );
        memcpy( *new_fortune, m_value.start, m_value.size );
        (*new_fortune)[m_value.size] = 0;
        LOG("Received SET request (%s)\n", *new_fortune);
    } else {
        LOG("Unknown command received: %.*s\n", (int)m_command.size, m_command.start );
        return -1;
    }
    return 0;
}



int main(int argc, char** argv)
{
    Options_t opts;
    int rc;

    pn_message_t *request_msg = pn_message();
    check( request_msg, "Failed to allocate a Message");
    pn_message_t *response_msg = pn_message();
    check( response_msg, "Failed to allocate a Message");
    pn_messenger_t *messenger = pn_messenger( 0 );
    check( messenger, "Failed to allocate a Messenger");

    fortune = _strdup("You killed Kenny!");
    check( fortune, "Out of memory" );

    DeduplicationDb_t *dupDb = DeduplicationDbNew( NULL, NULL );
    check( dupDb, "Unable to initialize duplication detection database." );

    parse_options( argc, argv, &opts );

    // no need to track outstanding messages.
    pn_messenger_set_outgoing_window( messenger, 0 );
    pn_messenger_set_incoming_window( messenger, 0 );

    pn_messenger_set_timeout( messenger, -1 );

    if (opts.gateway_addr) {
        LOG( "routing all messages via %s\n", opts.gateway_addr );
        rc = pn_messenger_route( messenger,
                                 "*", opts.gateway_addr );
        check( rc == 0, "pn_messenger_route() failed" );
    }

    pn_messenger_start(messenger);

    LOG("Subscribing to '%s'\n", opts.address);
    pn_subscription_t *subscription = pn_messenger_subscribe(messenger, opts.address);
    if (!subscription) check_messenger( messenger );

    for (;;) {

        LOG("Calling pn_messenger_recv(-1)\n");
        rc = pn_messenger_recv(messenger, -1);
        if (rc) check_messenger( messenger );

        DeduplicationPurgeExpired( dupDb );

        if (opts.delay) {
            LOG("Sleeping to delay response...\n");
            sleep( opts.delay );
        }

        LOG("Messages on incoming queue: %d\n", pn_messenger_incoming(messenger));
        while (pn_messenger_incoming(messenger)) {

            rc = pn_messenger_get( messenger, request_msg );
            check(rc == 0, "pn_messenger_get() failed");

            // decode the message
            command_t command = GET_COMMAND;
            char *new_fortune = NULL;
            const char *result = NULL;
            pn_bytes_t id = pn_data_get_string( pn_message_id( request_msg ) );
            if (id.size == 0 || id.size > 37) {
                LOG("Invalid message received - does not contain a valid msg id (uuid expected)\n" );
                result = "FAILED: invalid msg identifier";
            } else if (decode_request( request_msg, &command, &new_fortune )) {
                LOG("Invalid request message received!\n");
                result = "FAILED: invalid request";
            } else {
                LOG("Message contains a valid request.\n");

                // before processing it, check for a duplicate
                char msg_id[37];    // sizeof uuid as string of hex
                bool duplicate = false;
                memcpy( msg_id, id.start, id.size );
                msg_id[36] = 0;
                if (pn_message_get_delivery_count( request_msg ) != 0) {
                    LOG("Received retransmitted message\n");
                    if (DeduplicationIsDuplicate( dupDb, msg_id, NULL )) {
                        LOG("Duplicate found, skipping command.\n");
                        duplicate = true;
                    }
                }

                if (duplicate) {
                    result = "DUPLICATE";
                } else {
                    if (command == SET_COMMAND) {
                        LOG( "Setting fortune to \"%s\".\n", new_fortune );
                        free( fortune );
                        fortune = new_fortune;
                    }
                    result = "OK";
                }
                // since we don't know if the remote will ever get our
                // response, (re)remember this message in case the sender
                // re-transmits it
                DeduplicationRemember( dupDb, msg_id, NULL, _now() + opts.dup_timeout * 1000 );
            }

            if (result) {
                const char *reply_addr = pn_message_get_reply_to( request_msg );
                if (reply_addr) {
                    LOG("Sending reply...\n");
                    build_response_message( response_msg, reply_addr, command, result, fortune );
                    pn_data_copy( pn_message_correlation_id(response_msg),
                                  pn_message_correlation_id(request_msg) );
                    rc = pn_messenger_put( messenger, response_msg );
                    check(rc == 0, "pn_messenger_put() failed");
                }
            }
        }
    }

    rc = pn_messenger_stop(messenger);
    check(rc == 0, "pn_messenger_stop() failed");
    check_messenger(messenger);

    DeduplicationDbDelete( dupDb );

    pn_messenger_free(messenger);
    pn_message_free( request_msg );
    pn_message_free( response_msg );

    return 0;
}
