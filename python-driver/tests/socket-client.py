#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import optparse, sys, time, uuid
import re, socket, select, errno
import sockettransport
import proton

"""
This module implements a simple proton client which directly controls its
socket resources.
"""


class Peer(sockettransport.SocketTransport):
    """Represents a remote container for messages send by this client.
    """
    def __init__(self, name, socket, count, get_replies):

        super(Peer, self).__init__(socket, name)
        self.connection = proton.Connection()
        self.connection.container = name
        self.transport.bind(self.connection)
        self.sasl = proton.SASL(self.transport)
        self.sasl.mechanisms("ANONYMOUS")
        self.sasl.client()

        self.msg_count = count
        self.msgs_sent = 0
        self.replys_received = 0
        self.get_replies = get_replies

        # protocol setup example:

        self.connection.open()
        ssn = self.connection.session()
        ssn.open()
        # bi-directional links
        sender = ssn.sender("sender")
        #sender.target.address = "some-remote-target"
        #sender.source.address = self.connection.container + "/sending-link"
        sender.open()
        self._do_send(sender)  # kick it off
        if get_replies:
            receiver = ssn.receiver("receiver")
            #receiver.source.address = "some-remote-source"
            #receiver.target.address = self.connection.container + "/reply-to"
            receiver.open()
            receiver.flow(1)

    """
    Boilerplate Proton protocol processing.
    """
    def process_connection(self):

        NEED_INIT = proton.Endpoint.LOCAL_UNINIT
        NEED_CLOSE = (proton.Endpoint.LOCAL_ACTIVE|proton.Endpoint.REMOTE_CLOSED)

        # wait until SASL has authenticated
        if self.sasl:
            if self.sasl.state not in (proton.SASL.STATE_PASS,
                                       proton.SASL.STATE_FAIL):
                print("SASL wait.")
                return

            if self.sasl.state == proton.SASL.STATE_FAIL:
                # @todo connection failure - must notify the application!
                print("SASL failed for peer %s" % str(self.name))
                return

        # open all uninitialized endpoints

        if self.connection.state & NEED_INIT:
            print("Initializing connection")
            self.connection.open()

        ssn = self.connection.session_head(NEED_INIT)
        while ssn:
            print("Initializing session")
            ssn.open()
            ssn = ssn.next(NEED_INIT)

        link = self.connection.link_head(NEED_INIT)
        while link:
            # @todo: initialize terminus addresses???
            print("Initializing link")
            link.open()
            link = link.next(NEED_INIT)

        link = self.connection.link_head(NEED_CLOSE)
        while link:
            print("Link close")
            link.close()
            link = link.next(NEED_CLOSE)

        # process the work queue

        delivery = self.connection.work_head
        while delivery:
            if delivery.link.is_sender:
                self.send_update(delivery)
            else:
                self.recv_ready(delivery)
            delivery = delivery.work_next

        # close all endpoints closed by remotes

        ssn = self.connection.session_head(NEED_CLOSE)
        while ssn:
            print("session close")
            ssn.close()
            ssn = ssn.next(NEED_CLOSE)

        if self.connection.state == (NEED_CLOSE):
            print("conn close")
            self.connection.close()

    def send_update(self, delivery):
        """Callback to process the status of a previously-sent message.
        """
        print("Got a send update delivery! tag=%s" % delivery.tag)
        print("  writable=%s" % delivery.writable)
        print("  readable=%s" % delivery.readable)
        print("  updated=%s" % delivery.updated)
        print("  pending=%s" % delivery.pending)
        print("  partial=%s" % delivery.partial)
        print("  local_state=%s" % delivery.local_state)
        print("  remote_state=%s" % delivery.remote_state)
        print("  settled=%s" % delivery.settled)

        # do something... smart.
        sender = delivery.link
        # @todo: deal with remote terminal state, if necessary
        if delivery.settled:  # remote settled
            delivery.settle() # now so do we
            # and send another...
            if self.msgs_sent < self.msg_count:
                self._do_send(sender)

    def recv_ready(self, delivery):
        """Callback to handle an inbound delivery.
        """
        print("Got a recv_ready delivery! tag=%s" % delivery.tag)
        print("  writable=%s" % delivery.writable)
        print("  readable=%s" % delivery.readable)
        print("  updated=%s" % delivery.updated)
        print("  pending=%s" % delivery.pending)
        print("  partial=%s" % delivery.partial)
        print("  local_state=%s" % delivery.local_state)
        print("  remote_state=%s" % delivery.remote_state)
        print("  settled=%s" % delivery.settled)
        self._do_receive(delivery.link)

    def _do_send(self, sender):
        # hacky send example
        print("do_send")
        if sender.current:
            print "Current delivery = %s" % sender.current.tag

        if self.msgs_sent < self.msg_count:
            print "sender.credit %s" % sender.credit
            #if sender.credit == 0:
            #    print("requesting credit")
            #    sender.offered(self.msg_count - self.msgs_sent)

            msg = proton.Message()
            msg.address="amqp://0.0.0.0:5672"
            msg.subject="Hello World!"
            if self.get_replies:
                # messenger expects reply to in this format (huh?)
                msg.reply_to = "amqp://" + self.connection.container
            msg.body = "First OpenStack, then the WORLD!!!!"

            self.msgs_sent += 1
            delivery = sender.delivery( "%s" % self.msgs_sent )
            rc = sender.send( msg.encode() )
            print("rc=%s" % rc)
            sender.advance()  # indicates we are done writing to delivery
            ##delivery.settle()

    def _do_receive(self, receiver):
        """Process a received message"""
        print("do_recv")

        delivery = receiver.current
        if delivery and delivery.readable:
            # @todo what about partial?
            data = receiver.recv(delivery.pending)
            msg = proton.Message()
            msg.decode(data)
            print("Reply body=[%s]" % str(msg.body))
            receiver.advance()

            delivery.update(proton.Delivery.ACCEPTED)
            delivery.settle()

            self.replys_received += 1

        # should I grant more credit???
        if receiver.credit == 0:
            if self.replys_received < self.msg_count:
                receiver.flow(1)





def main(argv=None):

    _usage = """Usage: %prog [options] [<"Message text">]"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("-a", dest="targets", action="append", type="string",
                      help="<addr>[,<addr>]* \tThe target address(es) [amqp://<domain>[:<port>]]")
    parser.add_option("-c", dest="msg_count", type="int", default=1,
                      help="Send <n> messages to each target")
    parser.add_option("-R", dest="get_replies", action="store_true",
                      help="Wait for a reply from each message")

    opts, msg_text = parser.parse_args(args=argv)
    if not msg_text:
        msg_text = "Hey There!"
    if opts.targets is None:
        opts.targets = ["amqp://0.0.0.0:5672"]

    peers = sockettransport.SocketTransports()

    regex = re.compile(r"^amqp://([a-zA-Z0-9.]+)(:([\d]+))?$")
    for t in opts.targets:
        print("Connecting to %s" % str(t))
        x = regex.match(t)
        if not x:
            raise Exception("Bad address syntax: %s" % str(t))
        matches = x.groups()
        host = matches[0]
        port = int(matches[2]) if matches[2] else None
        addr = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        if not addr:
            print("Could not translate address '%s'" % str(t))
            continue
        s = socket.socket(addr[0][0], addr[0][1], addr[0][2])
        s.setblocking(0) # 0=non-blocking
        try:
            s.connect(addr[0][4])
        except socket.error, e:
            if e[0] != errno.EINPROGRESS:
                raise

        peers.add(Peer(uuid.uuid4().hex, s, opts.msg_count, opts.get_replies))

    sent = 0
    replies = 0
    done = False
    while not done:

        #
        # Poll for I/O & timers
        #

        readfd, writefd = peers.need_io()

        timeout = None
        deadline = peers.get_next_tick()
        if deadline:
            now = time.time()
            timeout = 0 if deadline <= now else deadline - now

        print("select start (t=%s)" % str(timeout))
        readable,writeable,ignore = select.select(readfd,writefd,[],timeout)
        print("select return")

        active_peers = peers.process_io(readable, writeable)

        #
        # Protocol processing
        #

        while active_peers:
            peer = active_peers.pop()
            peer.process_connection()
            # @todo check for closed connections and sockets!!!

    return 0


if __name__ == "__main__":
    sys.exit(main())

