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
    const char *server_addr;
    const char *gateway_addr;
    const char *new_message;
    unsigned int timeout;  // seconds
} Options_t;

static int log = 0;
#define LOG(...)                                        \
    if (log) { fprintf( stdout, __VA_ARGS__ ); }

static void usage(int rc)
{
    printf("Usage: f-client [OPTIONS] <f-server>\n"
           "Get the current fortune message from <f-server>\n"
           " <f-server> \tThe address of the fortune server [amqp[s]://domain[/name]]\n"
           " -S <message> \tSet the server's fortune message to \"<message>\"\n"
           " -G <gateway> \tGateway to use to reach <f-server>\n"
           " -t # \tInactivity timeout in seconds, 0 = no timeout [0]\n"
           );
    exit(rc);
}

static void parse_options( int argc, char **argv, Options_t *opts )
{
    int c;
    opterr = 0;

    memset( opts, 0, sizeof(*opts) );

    while ((c = getopt(argc, argv, "S:G:t:V")) != -1) {
        switch (c) {
        case 'S': opts->new_message = optarg; break;
        case 'G': opts->gateway_addr = optarg; break;
        case 't':
            if (sscanf( optarg, "%u", &opts->timeout ) != 1) {
                fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
                usage(1);
            }
            if (opts->timeout > 0) opts->timeout *= 1000;
        case 'V': log = 1; break;

        default:
            usage(1);
        }
    }

    if (optind < argc)
        opts->server_addr = argv[optind];
    else
        usage(2);
}


static void process_reply( pn_messenger_t *messenger,
                           pn_message_t *message)
{
    fprintf(stderr, "MESSAGE RECEIVED\n");
}


static int pn_data_vfill_wrapper(pn_data_t *data, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  int err = pn_data_vfill( data, fmt, ap );
  va_end(ap);
  return err;
}

static pn_message_t *build_set_message( const char *new_message )
{
    int rc;

    pn_message_t *message = pn_message();
    if (!message) return message;

    pn_data_t *body = pn_message_body(message);
    pn_data_clear( body );
    rc = pn_data_vfill_wrapper( body, "{SSSSS{SS}}",
                                "type", "request",
                                "command", "set",
                                "arguments", "new-message", new_message );
    check( rc == 0, "Failure to create set message" );


    return message;
}


static pn_message_t *build_get_message( void )
{
    int rc;

    pn_message_t *message = pn_message();
    if (!message) return message;

    pn_data_t *body = pn_message_body(message);
    pn_data_clear( body );
    rc = pn_data_vfill_wrapper( body, "{SSSS}",
                                "type", "request",
                                "command", "get" );

    check( rc == 0, "Failure to create get message" );


    return message;
}


/*
  type: request|response
  command: get|set
  arguments:  {message: <string>}
  status: <string>
*/


int main(int argc, char** argv)
{
    Options_t opts;
    int rc;

    pn_message_t *message = 0;
    pn_messenger_t *messenger = 0;

    parse_options( argc, argv, &opts );

    messenger = pn_messenger( argv[0] );

    pn_messenger_set_timeout( messenger, opts.timeout );
    pn_messenger_start(messenger);

    if (opts.new_message) {
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
    pn_message_set_address( message, opts.server_addr );
    pn_messenger_put(messenger, message);
    LOG("sending request...\n");
    rc = pn_messenger_recv(messenger, 1);
    check(rc == 0, "pn_messenger_recv() failed");
    rc = pn_messenger_get(messenger, message);
    check(rc == 0, "pn_messenger_get() failed");
    LOG("response received!\n");

    // type == response and status == "OK"
    // if command == get
    //    print message
    // else if command == set
    //    print message set to "message"

    process_reply( messenger, message);

    rc = pn_messenger_stop(messenger);
    check(rc == 0, "pn_messenger_stop() failed");
    check_messenger(messenger);

    pn_messenger_free(messenger);
    pn_message_free(message);

    return 0;
}
