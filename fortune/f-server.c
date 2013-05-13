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
#include <glib.h>
#include <uuid/uuid.h>

typedef struct {
    const char *address;
    const char *gateway_addr;
    unsigned int delay;
    unsigned int duration;      // for duplication detection
    unsigned int retry;
    unsigned int backoff;
    int send_timeout;  // seconds
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
           " -d <seconds> \tDelay <seconds> before processing and replying [0]\n"
           " -t # \tResponse send timeout in seconds [3]\n"
           " -l <seconds> \tDefault duration for saving received messages [60]\n"
           " -R # \tMessage send retry limit [0]\n"
           " -B <secs> \tSend retry backoff timeout [5]\n"
           " -V \tEnable debug logging\n"
           );
    exit(rc);
}

static void parse_options( int argc, char **argv, Options_t *opts )
{
    int c;
    opterr = 0;

    memset( opts, 0, sizeof(*opts) );
    opts->duration = 60;
    opts->send_timeout = 3;
    opts->backoff = 5;

    while ((c = getopt(argc, argv, "a:g:d:l:t:R:B:V")) != -1) {
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
            if (sscanf( optarg, "%u", &opts->duration ) != 1) {
                fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
                usage(1);
            }
            break;
        case 't':
            if (sscanf( optarg, "%d", &opts->send_timeout ) != 1) {
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

        default:
            usage(1);
        }
    }

    if (!opts->address) opts->address = "amqp://~0.0.0.0";
    if (opts->send_timeout > 0) opts->send_timeout *= 1000;
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
    pn_message_set_creation_time( message, msgr_now() );
    pn_message_set_delivery_count( message, 0 );

    pn_data_t *body = pn_message_body(message);
    pn_data_clear( body );
    int rc = pn_data_fill( body, "{SSSSSSSS}",
                           "type", "response",
                           "command", command == SET_COMMAND ? "set" : "get",
                           "value", value,
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


static GHashTable *deduplication_db;

typedef struct {
    char  id[37];  // key: UUID ascii len + 1
    pn_timestamp_t expire;
} DeDuplicateNode_t;


static void init_deduplication_db()
{
    deduplication_db = g_hash_table_new( g_str_hash, g_str_equal );
    check( deduplication_db, "Failed to initialize de-duplication hashtable." );
}

// remember the received message until "expire" time
//
static void remember_request( pn_message_t *message, pn_timestamp_t expire )
{
    pn_atom_t id = pn_message_get_id( message );
    if (id.type != PN_UUID) {
        fprintf(stderr, "Unexpected message id received: %d\n",
                (int) id.type);
        return;
    }

    char tmp[37];
    uuid_unparse_upper( *(uuid_t *)&id.u.as_uuid.bytes, tmp );
    LOG("Remembering message with id '%s'...\n", tmp );

    DeDuplicateNode_t *n = (DeDuplicateNode_t *) g_hash_table_lookup( deduplication_db, tmp );
    if (n) {   // already in table, update expire time
        LOG("... already present, updating expire time to %ul\n", (unsigned long) expire );
        n->expire = expire;
    } else {
        n = malloc( sizeof(DeDuplicateNode_t) );
        check( n, "Out of memory." );
        strcpy( n->id, tmp );
        n->expire = expire;
        g_hash_table_insert( deduplication_db, n->id, n );
    }
}


// has message already been seen?
//
static bool is_duplicate( pn_message_t *message, pn_timestamp_t refresh )
{
    pn_atom_t id = pn_message_get_id( message );
    if (id.type != PN_UUID) {
        fprintf(stderr, "Unexpected message id received: %d\n",
                (int) id.type);
        return false;
    }

    char tmp[37];
    uuid_unparse_upper( *(uuid_t *)&id.u.as_uuid.bytes, tmp );

    DeDuplicateNode_t *n = (DeDuplicateNode_t *) g_hash_table_lookup( deduplication_db, tmp );
    if (n) {
        if (n->expire <= msgr_now()) {
            LOG( "expiring old message from deduplication database: %s\n", tmp );
            g_hash_table_remove( deduplication_db, n->id );
            free( n );
            n = NULL;
        } else {
            n->expire = refresh;
        }
    }

    return n != NULL;
}


// remove the message from the deduplication database
//
static void forget_request( pn_message_t *message )
{
    pn_atom_t id = pn_message_get_id( message );
    if (id.type != PN_UUID) {
        fprintf(stderr, "Unexpected message id received: %d\n",
                (int) id.type);
        return;
    }

    char tmp[37];
    uuid_unparse_upper( *(uuid_t *)&id.u.as_uuid.bytes, tmp );

    DeDuplicateNode_t *n = (DeDuplicateNode_t *) g_hash_table_lookup( deduplication_db, tmp );
    if (n) {
        LOG( "removing old message from deduplication database: %s\n", tmp );
        g_hash_table_remove( deduplication_db, n->id );
        free( n );
    }
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

    fortune = msgr_strdup("You killed Kenny!");
    check( fortune, "Out of memory" );

    init_deduplication_db();

    parse_options( argc, argv, &opts );

    // only 1 message (request/response) outstanding at a time
    pn_messenger_set_outgoing_window( messenger, 1 );
    pn_messenger_set_incoming_window( messenger, 1 );

    pn_messenger_set_timeout( messenger, -1 );

    if (opts.gateway_addr) {
        LOG( "routing all messages via %s\n", opts.gateway_addr );
        rc = pn_messenger_route( messenger,
                                 "*", opts.gateway_addr );
        check( rc == 0, "pn_messenger_route() failed" );
    }

    pn_messenger_start(messenger);

    LOG("Subscribing to '%s'\n", opts.address);
    pn_messenger_subscribe(messenger, opts.address);

    for (;;) {

        LOG("Calling pn_messenger_recv(%d)\n", 1);
        rc = pn_messenger_recv(messenger, 1);
        check(rc == 0, "pn_messenger_recv() failed");

        LOG("Messages on incoming queue: %d\n", pn_messenger_incoming(messenger));
        while (pn_messenger_incoming(messenger)) {

            rc = pn_messenger_get( messenger, request_msg );
            check(rc == 0, "pn_messenger_get() failed");
            pn_tracker_t request_tracker = pn_messenger_incoming_tracker( messenger );

            if (opts.delay) {
                LOG("Sleeping to delay response...\n");
                sleep( opts.delay );
            }

            // decode the message
            command_t command = GET_COMMAND;
            char *new_fortune = NULL;
            bool send_reply = false;
            rc = decode_request( request_msg, &command, &new_fortune );
            if (rc) {
                LOG("Invalid message received - rejecting\n");
                rc = pn_messenger_reject(messenger, request_tracker, 0 );
                check( rc == 0, "pn_messenger_reject() failed");
            } else {
                LOG("Message is valid, accepting it.\n");
                rc = pn_messenger_accept(messenger, request_tracker, 0 );
                check( rc == 0, "pn_messenger_accept() failed");
                send_reply = true;
            }

            // before processing it, check for a duplicate
            bool duplicate = false;
            if (pn_message_get_delivery_count( request_msg ) != 0) {
                LOG("Received retransmitted message\n");
                if (is_duplicate( request_msg, msgr_now() + opts.duration * 1000 )) {
                    LOG("Duplicate found, skipping command.\n");
                    duplicate = true;
                }
            }

            if (command == SET_COMMAND && !duplicate) {
                free( fortune );
                fortune = new_fortune;
            }

            // BEGIN HACK: before we can settle, we need to wait for
            // the sender to settle first.  See amqp-1.0 spec -
            // exactly once delivery
            LOG("waiting for sender to settle the request...\n");
            pn_messenger_set_timeout( messenger, 500 );
            pn_messenger_recv( messenger, -1 );
            pn_messenger_set_timeout( messenger, -1 );
            // END HACK
            if (pn_messenger_status( messenger, request_tracker) != PN_STATUS_ACCEPTED) {
                fprintf(stderr, "Remote did not settle as expected! %d\n",
                        (int)pn_messenger_status( messenger, request_tracker));

                // since we don't know if the remote ever got our
                // outcome, remember this message in case the sender
                // re-transmits it
                remember_request( request_msg, msgr_now() + opts.duration * 1000 );
            } else if (duplicate) {
                forget_request( request_msg );
            }

            LOG("settling the request locally...\n");
            rc = pn_messenger_settle( messenger, request_tracker, 0 );
            check(rc == 0, "pn_messenger_settle() failed");

            if (send_reply) {
                //
                // Send reponse
                //
                const char *reply_addr = pn_message_get_reply_to( request_msg );
                if (reply_addr) {
                    build_response_message( response_msg, reply_addr, command, "OK", fortune );

                    DeliveryStatus_t ds = deliver_message( messenger,
                                                           response_msg,
                                                           opts.retry, opts.send_timeout, opts.backoff );
                    if (ds != STATUS_ACCEPTED) {
                        fprintf( stderr, "Send of response failed - retries exhausted.\n" );
                    }
                }
            }
        }
    }

    rc = pn_messenger_stop(messenger);
    check(rc == 0, "pn_messenger_stop() failed");
    check_messenger(messenger);

    pn_messenger_free(messenger);
    pn_message_free( request_msg );
    pn_message_free( response_msg );

    return 0;
}
