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
} Options_t;

static int log = 0;
#define LOG(...)                                        \
    if (log) { fprintf( stdout, __VA_ARGS__ ); }

static char *fortune;

static void usage(int rc)
{
    printf("Usage: f-server [OPTIONS] \n"
           " -a <addr> \tAddress to listen on [amqp://~0.0.0.0]\n"
           " -V \tEnable debug logging\n"
           );
    exit(rc);
}

static void parse_options( int argc, char **argv, Options_t *opts )
{
    int c;
    opterr = 0;

    memset( opts, 0, sizeof(*opts) );

    while ((c = getopt(argc, argv, "a:V")) != -1) {
        switch (c) {
        case 'a': opts->address = optarg; break;
        case 'V': log = 1; break;

        default:
            usage(1);
        }
    }

    if (!opts->address) opts->address = "amqp://~0.0.0.0";
}


static void build_reply_message( pn_message_t *message, const char *command,
                                const char *status, const char *value )
{
    pn_data_t *body = pn_message_body(message);
    pn_data_clear( body );
    int rc = pn_data_fill( body, "{SSSSSSSS}",
                           "type", "response",
                           "command", command,
                           "value", value,
                           "status", status );
    check( rc == 0, "Failure to create response message" );
}


static int process_message( pn_messenger_t *messenger,
                            pn_message_t *message )
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
    if (strncmp("request", m_type.start, m_type.size)) {
        LOG("Unknown message type received: %.*s\n", (int)m_type.size, m_type.start );
        // should try to reply with error status, for now I punt
        return 0;
    }
    if (strncmp("get", m_command.start, m_command.size) == 0) {
        LOG("Received GET request\n");
        build_reply_message( message, "get", "OK", fortune );
    } else {  // assume set
        char *new_fortune = (char *) malloc(sizeof(char) * (m_value.size + 1));
        check( new_fortune, "Out of memory" );
        memcpy( new_fortune, m_value.start, m_value.size );
        new_fortune[m_value.size] = 0;
        LOG("Received SET request (%s)\n", new_fortune);
        free( fortune );
        fortune = new_fortune;
        build_reply_message( message, "set", "OK", fortune );
    }

    const char *reply_addr = pn_message_get_reply_to( message );
    if (reply_addr) {
        pn_message_set_address( message, reply_addr );
        pn_message_set_creation_time( message, msgr_now() );
        return 1;
    }
    return 0;  // don't reply
}


int main(int argc, char** argv)
{
    Options_t opts;
    int rc;

    pn_message_t *message;
    pn_messenger_t *messenger;

    fortune = msgr_strdup("You killed Kenny!");
    check( fortune, "Out of memory" );

    parse_options( argc, argv, &opts );

    message = pn_message();
    messenger = pn_messenger( 0 );

    pn_messenger_start(messenger);
    check_messenger(messenger);

    pn_messenger_subscribe(messenger, opts.address);
    check_messenger(messenger);
    LOG("Subscribing to '%s'\n", opts.address);

    for (;;) {

        LOG("Calling pn_messenger_recv(%d)\n", -1);
        rc = pn_messenger_recv(messenger, -1);
        check(rc == 0, "pn_messenger_recv() failed");

        LOG("Messages on incoming queue: %d\n", pn_messenger_incoming(messenger));
        while (pn_messenger_incoming(messenger)) {
            rc = pn_messenger_get(messenger, message);
            check(rc == 0, "pn_messenger_get() failed");
            if (process_message( messenger, message )) {
                LOG("Replying to: %s\n", pn_message_get_address(message) );
                rc = pn_messenger_put(messenger, message);
                check(rc == 0, "pn_messenger_put() failed");
            }
        }
    }

    // this will flush any pending sends
    if (pn_messenger_outgoing(messenger) > 0) {
        LOG("Calling pn_messenger_send()\n");
        rc = pn_messenger_send(messenger);
        check(rc == 0, "pn_messenger_send() failed");
    }

    rc = pn_messenger_stop(messenger);
    check(rc == 0, "pn_messenger_stop() failed");
    check_messenger(messenger);

    pn_messenger_free(messenger);
    pn_message_free(message);

    return 0;
}
