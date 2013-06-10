This directory contains an example RPC-like client and server running
over Proton Messenger.

This example is in the form of a simple "fortune" server.  The server
will store a simple text message - the fortune - and provide clients
access to it.  Clients can request to 'get' the current fortune, or
'set' the fortune to a new value.

RPC messages are implemented as simple AMQP 1.0 maps that contain the
type of message (request or response), and any additional parameters.

The Client (f-client.c):

The client sends a request message to the server.  Should the send
operation fail with an unknown delivery status, the client will retry
sending the message.  Once the message has been sent successfully, or
the total number of retries exhausted, the client will attempt to
receive the response.

There are various tunable parameters that affect how the client
behaves, like the maximum number of send retries, timeouts, etc.


The Server (f-server.c):

The server will process request messages sent by the clients, and send
a response message to each valid request.  Invalid requests are
rejected.  The server must deal with handling duplicate requests.
This is done by keeping a database of received messages, which is used
to filter out already received messages.  Messages are identified
using the "message-id" field, which is described in the AMQP-1.0
specification.


Guaranteed Delivery:

This implementation attempts to guarantee delivery of the message by
manually retrying requests and filtering duplicates.  All retry logic
is implemented in the application code itself - there is no use of
Proton delivery status or trackers.



BUILDING
--------

See the top-level README.txt for build instructions.

RUNNING
-------

A successful build should result in a server executable (f-server),
and a client executable (f-client).

Run the server:

./f-server -a amqp://~0.0.0.0 -V

And in a different shell, run the client:

./f-client -a amqp://0.0.0.0 -V

The "-V" turns on debug logging to stdout - it's optional.

## Running with dispatch-router

./f-server -a amqp://0.0.0.0:5672/SERVER
./f-client -a amqp://0.0.0.0:5672/SERVER -r amqp://0.0.0.0:5672/CLIENT




