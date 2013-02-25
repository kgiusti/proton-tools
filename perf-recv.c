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

#include "proton/message.h"
#include "proton/messenger.h"

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/time.h>
#include <string.h>

#define check(messenger)                                         \
  {                                                              \
    if(pn_messenger_errno(messenger))                            \
    {                                                            \
      die(__FILE__, __LINE__, pn_messenger_error(messenger));    \
    }                                                            \
  }                                                              \

void die(const char *file, int line, const char *message)
{
  fprintf(stderr, "%s:%i: %s\n", file, line, message);
  exit(1);
}

typedef struct options_t {
  char *address;
  uint64_t msg_count;
  int32_t credit;
  int window;
  char *certificate;
  char *privatekey;
  char *password;
} options_t;

static void usage(int rc)
{
  printf("Usage: recv [options] <addr>\n");
  printf("-a    \tAddress to listen on [amqp://~0.0.0.0]\n");
  printf("-c    \tNumber of messages to receive [0=forever]\n");
  printf("-r    \t# messages per call to recv [2048]\n");
  printf("-w    \tSize for incoming window\n");
  printf("-C    \tPath to the certificate file.\n");
  printf("-K    \tPath to the private key file.\n");
  printf("-P    \tPassword for the private key.\n");
  exit(rc);
}

static void parse_options( int argc, char **argv, options_t *opts )
{
  int c;
  opterr = 0;

  memset( opts, 0, sizeof(*opts) );
  opts->address = "amqp://~0.0.0.0";
  opts->credit = 2048;

  while((c = getopt(argc, argv, "ha:c:r:w:C:K:P:")) != -1)
  {
    switch(c)
    {
    case 'a': opts->address = optarg; break;
    case 'c':
      if (sscanf( optarg, "%lu", &opts->msg_count ) != 1) {
        fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
        usage(1);
      }
      break;
    case 'r':
      if (sscanf( optarg, "%d", &opts->credit ) != 1) {
        fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
        usage(1);
      }
      break;
    case 'w':
      if (sscanf( optarg, "%d", &opts->window ) != 1) {
        fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
        usage(1);
      }
      break;

    case 'C': opts->certificate = optarg; break;
    case 'K': opts->privatekey = optarg; break;
    case 'P': opts->password = optarg; break;

    default:
      usage(1);
    }
  }
}

//sigh
pn_timestamp_t pn_i_now()
{
  struct timeval now;
  if (gettimeofday(&now, NULL)) abort();
  return ((pn_timestamp_t)now.tv_sec) * 1000 + (now.tv_usec / 1000);
}


int main(int argc, char** argv)
{
  options_t opts;
  pn_message_t *message;
  pn_messenger_t *messenger;

  parse_options( argc, argv, &opts );

  message = pn_message();
  messenger = pn_messenger(argv[0]);

  /* load the various command line options if they're set */
  if(opts.certificate)
  {
    pn_messenger_set_certificate(messenger, opts.certificate);
  }

  if(opts.privatekey)
  {
    pn_messenger_set_private_key(messenger, opts.privatekey);
  }

  if(opts.password)
  {
    pn_messenger_set_password(messenger, opts.password);
  }

  if (opts.window) {
    // RAFI: seems to cause receiver to hang:
    pn_messenger_set_incoming_window( messenger, opts.window );
  }

  pn_messenger_start(messenger);
  check(messenger);

  pn_messenger_subscribe(messenger, opts.address);
  check(messenger);

  uint64_t count = 0;
  pn_timestamp_t start = 0;

  if (opts.msg_count) {
    // start the timer only after receiving the first msg
    pn_messenger_recv(messenger, 1);
    start = pn_i_now();
    count++;
  }

  while (!opts.msg_count || count < opts.msg_count) {

    pn_messenger_recv(messenger, (opts.credit ? opts.credit : -1));
    check(messenger);

    while (pn_messenger_incoming(messenger))
    {
      if (pn_messenger_get(messenger, message))
        abort();
      // TODO: header decoding?
      count++;
    }
  }

  pn_timestamp_t end = pn_i_now() - start;

  pn_messenger_stop(messenger);
  pn_messenger_free(messenger);
  pn_message_free(message);

  double secs = end/(double)1000.0;
  fprintf(stdout, "Total time %f sec (%f msgs/sec)\n",
          secs, opts.msg_count/secs);

  return 0;
}
