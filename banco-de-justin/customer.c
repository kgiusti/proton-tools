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
    unsigned int ttl;
    int timeout;  // milliseconds
    int transaction;  // +deposit/-withdrawal
} Options_t;

static void usage(int rc)
{
    printf("Usage: customer [OPTIONS] -- <-withdrawal/+deposit>\n"
           "Perform a transaction with the bank\n"
           " -a <address> \tThe address of the bank server [amqp://0.0.0.0]\n"
           " -g <gateway> \tGateway to use to reach the bank server\n"
           " -t # \tTimeout in seconds [10]\n"
           " -l <secs> \tTTL to set in message, 0 = no TTL [0]\n"
           " -V \tEnable debug logging\n"
           );
    exit(rc);
}

static void parse_options( int argc, char **argv, Options_t *opts )
{
    int c;
    opterr = 0;

    memset( opts, 0, sizeof(*opts) );
    opts->timeout = 10;

    while ((c = getopt(argc, argv, "a:g:t:l:V")) != -1) {
        switch (c) {
        case 'a': opts->address = optarg; break;
        case 'g': opts->gateway_addr = optarg; break;
        case 't':
            if (sscanf( optarg, "%d", &opts->timeout ) != 1) {
                fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
                usage(1);
            }
            break;
        case 'l':
            if (sscanf( optarg, "%u", &opts->ttl ) != 1) {
                fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
                usage(1);
            }
            break;
        case 'V': enable_logging(); break;

        default:
            usage(1);
        }
    }

    if (!opts->address) opts->address = "amqp://0.0.0.0";
    if (opts->timeout > 0) opts->timeout *= 1000;

    if (optind >= argc || sscanf( argv[optind], "%d", &opts->transaction ) != 1) {
        usage(1);
    }
}



int main(int argc, char** argv)
{
    Options_t opts;
    int rc;

    pn_message_t *request_msg = pn_message();
    check( request_msg, "Failed to allocate a Message");
    pn_messenger_t *messenger = pn_messenger( 0 );
    check( messenger, "Failed to allocate a Messenger");

    parse_options( argc, argv, &opts );

    pn_messenger_set_outgoing_window( messenger, 1 );
    pn_messenger_set_timeout( messenger, opts.timeout );

    if (opts.gateway_addr) {
        LOG( "routing all messages via %s\n", opts.gateway_addr );
        rc = pn_messenger_route( messenger,
                                 "*", opts.gateway_addr );
        check( rc == 0, "pn_messenger_route() failed" );
    }

    pn_messenger_start(messenger);

    // Create a request message
    //
    LOG( "Requesting transaction: %d dollars.\n", opts.transaction );
    pn_message_set_address( request_msg, opts.address );
    pn_message_set_delivery_count( request_msg, 0 );
    if (opts.ttl)
        pn_message_set_ttl( request_msg, opts.ttl * 1000 );
    pn_data_t *body = pn_message_body( request_msg );
    pn_data_clear( body );
    rc = pn_data_put_int( body, opts.transaction );
    check( rc == 0, "Failure to create request message" );

    // and send it
    //
    pn_status_t status = deliver_message( messenger, request_msg );

    if (status == PN_STATUS_ACCEPTED) {
        fprintf( stdout, "%s of %d dollars succeeded!\n",
                 opts.transaction < 0 ? "Widthdrawal" : "Deposit",
                 opts.transaction );
    } else {
        fprintf( stdout, "%s of %d dollars FAILED!  Error code=%d\n",
                 opts.transaction < 0 ? "Widthdrawal" : "Deposit",
                 opts.transaction, rc );
    }

    rc = pn_messenger_stop(messenger);
    check(rc == 0, "pn_messenger_stop() failed");

    pn_messenger_free(messenger);
    pn_message_free( request_msg );

    return 0;
}
