This directory contains a client/server bank account demo.  The server
maintains the back account balance, and clients may deduct or deposit
against the balance.

What makes this demo unique is that it does not implement a
traditional request/response pattern.  Instead, the clients send
requests to the server but rely on the message's delivery status to
determine if the request was handled or not.

Proton Messenger provides a delivery status for each message sent by
an application.  This status is used by the sender to determine the
disposition of the message at the receiver.  (Well, actually, this
really isn't 100% correct - AMQP 1.0 only defines this for the link
termination point, which -may- not be the message's destination).

The receiver can set this status to "accepted" or "rejected".  This
demo uses this status to determine if the client's transaction was
successful or not.


The Client:

The client sends a request message containing a 'deposit' or
'withdrawl' operation to the server (bank).  The client monitors the
status of the request using the delivery status provided by Proton.
Should the delivery status indicate a failure, the client reports the
failure and exits.


The Server:

The server will process request messages sent by the clients.
Withdrawl requests that would result in the account being overdrawn
will cause the server to reject the received message.


Guaranteed Delivery:

This implementation attempts to use the delivery status of send and
received messages in order to guarantee delivery.  It does this
poorly.  The basic idea was to model the "3-Ack" procedure described
in the AMQP-1.0 specification (see section 2.6.12 Transferring A
Message).  The implemented behavior is described below:

Sending:

Once a message is sent, the sender checks the remote state via the
pn_tracker_t object associated with the message.  If the remote state
is ACCEPTED, the sender considers the request successful.  If the
remote state is REJECTED, the sender considers the request as
permanently failed, and will not retry.  If the remote state is
unknown (after a timeout period), the sender will abort the operation.

Receiving:

Once a message is received, it is validated.  The result of that
validation is used to determine the outcome of the receipt: either
ACCEPTED or REJECTED.  This outcome is then set using the message's
associated pn_tracker_t.

RUNNING
-------

Run the server:

./bank -a amqp://~0.0.0.0

And in a different shell, run the client:

./customer -a amqp://0.0.0.0




