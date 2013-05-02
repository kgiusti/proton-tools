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
#include <string.h>
#include <ctype.h>
#include <sys/time.h>

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
  uint32_t msg_size;
  uint32_t add_headers;
  uint32_t put_count;
  int   window;
} options_t;

static void usage(int rc)
{
  printf("Usage: send [-a addr] \n");
  printf("-a     \tThe target address [amqp[s]://domain[/name]]\n");
  printf("-c     \tNumber of messages to send [500000]\n");
  printf("-s     \tSize of message body in bytes [1024]\n");
  printf("-p     \t*TODO* Add N sample properties to each message [3]\n");
  printf("-b     \t# messages to put before calling send [1024]\n");
  printf("-w    \tSize for outgoing window\n");
  exit(rc);
}

static void parse_options( int argc, char **argv, options_t *opts )
{
  int c;
  opterr = 0;

  memset( opts, 0, sizeof(*opts) );
  opts->address = "amqp://0.0.0.0";
  opts->msg_count = 5000000;
  opts->msg_size  = 1024;
  opts->add_headers = 3;
  opts->put_count = 1024;

  while((c = getopt(argc, argv, "a:c:s:p:b:w:")) != -1) {
    switch(c) {
    case 'a': opts->address = optarg; break;
    case 'c':
      if (sscanf( optarg, "%lu", &opts->msg_count ) != 1) {
        fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
        usage(1);
      }
      break;
    case 's':
      if (sscanf( optarg, "%u", &opts->msg_size ) != 1) {
        fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
        usage(1);
      }
      break;
    case 'p':
      if (sscanf( optarg, "%u", &opts->add_headers ) != 1) {
        fprintf(stderr, "Option -%c requires an integer argument.\n", optopt);
        usage(1);
      }
    case 'b':
      if (sscanf( optarg, "%u", &opts->put_count ) != 1) {
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
    default:
      usage(1);
    }
  }
}


//sigh - would be nice if proton exported this:
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
  pn_data_t *body = pn_message_body(message);
  char *data = calloc(1, opts.msg_size);
  pn_data_put_binary(body, pn_bytes(opts.msg_size, data));
  free(data);

  // TODO: how do we effectively benchmark header processing overhead???
  pn_data_t *props = pn_message_properties(message);
  pn_data_put_map(props);
  pn_data_enter(props);
  //
  pn_data_put_string(props, pn_bytes(6,  "string"));
  pn_data_put_string(props, pn_bytes(10, "this is awkward"));
  //
  pn_data_put_string(props, pn_bytes(4,  "long"));
  pn_data_put_long(props, 12345);
  //
  pn_data_put_string(props, pn_bytes(9, "timestamp"));
  pn_data_put_timestamp(props, (pn_timestamp_t) 54321);
  pn_data_exit(props);
  pn_message_set_address(message, opts.address);


  messenger = pn_messenger( argv[0] );
  if (opts.window) {
    pn_messenger_set_outgoing_window( messenger, opts.window );
  }
  pn_messenger_start(messenger);

  pn_timestamp_t start = pn_i_now();

  for (uint64_t i = 1; i <= opts.msg_count; ++i) {

    pn_messenger_put(messenger, message);
    if (opts.put_count > 0 && (i % opts.put_count == 0) ) {
      pn_messenger_send(messenger);
    }
  }
  pn_messenger_send(messenger);

  pn_timestamp_t end = pn_i_now() - start;

  pn_messenger_stop(messenger);
  pn_messenger_free(messenger);
  pn_message_free(message);

  double secs = end/(double)1000.0;
  fprintf(stdout, "Total time %f sec (%f msgs/sec)\n",
          secs, opts.msg_count/secs);

  return 0;
}
