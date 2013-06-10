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
    int balance;
} Options_t;

static void usage(int rc)
{
    printf("Usage: f-server [OPTIONS] <starting-balance>\n"
           " -a <addr> \tAddress to listen on [amqp://~0.0.0.0]\n"
           " -g <gateway> \tGateway for sending all reply messages\n"
           " -d <seconds> \tSimulate delay by sleeping <seconds> before replying [0]\n"
           " -V \tEnable debug logging\n"
           );
    exit(rc);
}

static void parse_options( int argc, char **argv, Options_t *opts )
{
    int c;
    opterr = 0;

    memset( opts, 0, sizeof(*opts) );

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
        case 'V': enable_logging(); break;

        default:
            usage(1);
        }
    }

    if (!opts->address) opts->address = "amqp://~0.0.0.0";
    if (optind >= argc || sscanf( argv[optind], "%d", &opts->balance ) != 1) {
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
    pn_subscription_t *subscription = pn_messenger_subscribe(messenger, opts.address);
    if (!subscription) check_messenger( messenger );

    for (;;) {
        LOG("Waiting for a transaction...\n");
        rc = pn_messenger_recv(messenger, -1);
        if (rc) check_messenger( messenger );

        if (opts.delay) {
            LOG("Sleeping to delay response...\n");
            sleep( opts.delay );
        }

        LOG("Messages on incoming queue: %d\n", pn_messenger_incoming(messenger));
        while (pn_messenger_incoming(messenger)) {

            rc = pn_messenger_get( messenger, request_msg );
            check(rc == 0, "pn_messenger_get() failed");

            pn_tracker_t tracker = pn_messenger_incoming_tracker( messenger );

            pn_data_t *body = pn_message_body( request_msg );

            LOG("pn_data_size = %lu\n", (unsigned long)pn_data_size( body ));

            if (!pn_data_next( body ) || pn_data_type( body ) != PN_INT) {
                LOG("Transaction failed - invalid message format!\n");
                rc = pn_messenger_reject( messenger, tracker, 0 );
            } else {
                int transaction = pn_data_get_int( body );

                LOG("Client request: %s %d dollars\n",
                    (transaction < 0) ? "Withdrawal" : "Deposit",
                    abs(transaction));

                if (transaction < 0 && -transaction > opts.balance) {
                    LOG("Transaction failed - would result in overdrawn account!\n");
                    rc = pn_messenger_reject( messenger, tracker, 0 );
                } else {
                    opts.balance += transaction;
                    rc = pn_messenger_accept( messenger, tracker, 0);
                    LOG("Transaction complete - current balance = %d dollars\n", opts.balance);
                }
            }
            // Now what?
        }
    }

    rc = pn_messenger_stop(messenger);
    check(rc == 0, "pn_messenger_stop() failed");
    check_messenger(messenger);

    pn_messenger_free(messenger);
    pn_message_free( request_msg );

    return 0;
}
