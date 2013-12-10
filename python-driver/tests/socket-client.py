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





class EndpointEventHandler(object):
    """State machine interface for generic Proton endpoints
    """
    def __init__(self, endpoint):
        self._endpoint = endpoint

    def uninit(self):
        """Called when local state is UNINIT."""
        self._endpoint.open()

    def active(self):
        """Called while both local and remote states are ACTIVE."""
        pass  # type specific

    def remoteClosed(self):
        """Called when local state is ACTIVE and remote state is CLOSED."""
        self._endpoint.close()

    def closed(self):
        """Called while both local and remote states are CLOSED."""
        pass # optional


class ReceiverEventHandler(EndpointEventHandler):
    """Process state changes to a Proton Receive Link."""
    def __init__(self, link):
        super(ReceiverEventHandler, self).__init__(link)

    def active(self):
        pass

    def closed(self):
        pass


class SenderEventHandler(EndpointEventHandler):
    """Process state changes to a Proton Sender Link."""
    def __init__(self, link):
        super(SenderEventHandler, self).__init__(link)

    def active(self):
        pass

    def closed(self):
        pass



class Peer(sockettransport.SocketTransport):
    """Represents a remote destination for messages send by this client.
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
        #sender.target.address = "%s/%s" % (self.connection.container,"TARGET")
        sender.target.address = "some-remote-target"
        sender.source.address = self.connection.container + "/sending-link"
        sender.open()
        self._do_send(sender)  # kick it off
        if get_replies:
            receiver = ssn.receiver("receiver")
            #receiver.source.address = "%s/%s" % (self.connection.container, "SOURCE")
            receiver.source.address = "some-remote-source"
            receiver.target.address = self.connection.container + "/reply-to"
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
                msg.reply_to = self.connection.container + "/reply-to"
                #msg.reply_to = "%s/%s" % (self.connection.container, "SOURCE")
            msg.body = "First OpenStack, then the WORLD!!!!"


            self.msgs_sent += 1
            delivery = sender.delivery( "%s" % self.msgs_sent )
            rc = sender.send( msg.encode() )
            print("rc=%s" % rc)
            sender.advance()  # indicates we are done writing to delivery
            ##delivery.settle()


    #                     delivery = pn_delivery(d.sender, "delivery-%d" %
    #                                            d.msgs_pending)
    #                     rc = pn_link_send(d.sender, "message")
    #                     assert(rc >= 0)
    #                     d.msgs_pending -= 1
    #                     pn_link_advance(d.sender)
    #                     pn_delivery_settle(delivery)



    def _do_receive(self, receiver):
        print("do_recv")

        if receiver.current:
            print "Current delivery = %s" % receiver.current.tag

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
    #parser.add_option("-H", dest="hostname",
    #                  help="Address to listen on for responses")

    opts, msg_text = parser.parse_args(args=argv)
    if not msg_text:
        msg_text = "Hey There!"
    if opts.targets is None:
        opts.targets = ["amqp://0.0.0.0:5672"]

    # listener = None
    # reply_addr = None
    # hostname = opts.hostname || socket.gethostname()
    # listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # listener.setblocking(0) # non-blocking
    # listener.bind((socket.getfqdn(hostname),0))
    # reply_addr = "amqp://%s:%d" % listener.getsockname()
    # listener.listen(5)

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

        # ssn = pn_session(conn)
        # slink = pn_sender(ssn, "sender")
        # dst = pn_link_target(slink)
        # pn_terminus_set_address(dst, "socket-target")
        # rlink = None
        # if opts.get_replies:
        #     rlink = pn_receiver(ssn, "receiver")
        #     src = pn_link_src(rlink)
        #     pn_terminus_set_address(src, "socket-source")

        # now open all the engine endpoints
        #pn_connection_open(conn)
        #pn_session_open(ssn)
        #pn_link_open(slink)
        #pn_link_open(rlink)

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

    #         #########################


    #         if ((d.sender.link_state() & (PN_LOCAL_ACTIVE | PN_REMOTE_ACTIVE)) == (PN_LOCAL_ACTIVE | PN_REMOTE_ACTIVE)):
    #             if d.msgs_pending:
    #                 while pn_link_credit(d.sender) > 0 and d.msgs_pending:
    #                     delivery = pn_delivery(d.sender, "delivery-%d" %
    #                                            d.msgs_pending)
    #                     rc = pn_link_send(d.sender, "message")
    #                     assert(rc >= 0)
    #                     d.msgs_pending -= 1
    #                     pn_link_advance(d.sender)
    #                     pn_delivery_settle(delivery)
    #                 if pn_link_credit(d.sender) == 0:
    #                     if d.msgs_pending > 0:
    #                         pn_link_offer(d.sender, d.msgs_pending)
    #             else:
    #                 d.sender.close()
    #         elif ((d.sender.link.state() & PN_REMOTE_CLOSED)):
    #             if (d.sender.link.state() & PN_LOCAL_ACTIVE):
    #                 d.sender.close()

    #         if ((d.receiver.link_state() & (PN_LOCAL_ACTIVE | PN_REMOTE_ACTIVE)) == (PN_LOCAL_ACTIVE | PN_REMOTE_ACTIVE)):

    #             while pn_link_queued(d.receiver) > 0:
    #                 delivery = pn_link_current(d.receiver)
    #                 # read all bytes of message
    #                 rc, msg = pn_link_recv(receiver.link,
    #                                        pn_delivery_pending(delivery))
    #                 assert(rc >= 0)
    #                 d.replies_received += 1
    #                 pn_delivery_update(delivery, PN_ACCEPTED)
    #                 pn_link_advance(d.receiver)
    #                 pn_delivery_settle(delivery)

    #             if d.replies_received < opts.msg_count:
    #                 if pn_link_remote_credit(d.receiver) == 0:
    #                     pn_link_flow(d.receiver, 1)
    #             else:
    #                 d.receiver.close()
    #         elif ((d.receiver.link.state() & PN_REMOTE_CLOSED)):
    #             if (d.receiver.link.state() & PN_LOCAL_ACTIVE):
    #                 d.receiver.close()



    #     # if connection down, mark destination done
    #     # if msgs remain and credit, send msg
    #     # if msgs remain and no credit, send offer
    #     # if no msg remain, send drained
    #     # once all sent msgs settled, sender done
    #     # if replies and replies available
    #     #   recv and settle them
    #     #   once all recv deliveries settled, recv done

    # # wait until we authenticate with the server
    # while pn_sasl_state(sender.sasl) not in (PN_SASL_PASS, PN_SASL_FAIL):
    #     sender.wait()
    #     if sender.closed():
    #         sender.log("connection failed")
    #         return -1;

    # if pn_sasl_state(sender.sasl) == PN_SASL_FAIL:
    #     print("Error: Authentication failure")
    #     return -1



    # sent = 0
    # while send < opts.msg_count:
    # pendingSends = list(options.messages)
    # while pendingSends:
    #     # wait until the server grants us some send credit
    #     if pn_link_credit(sender.link) == 0:
    #         sender.log("wait for credit")
    #         sender.wait()

    #     while pn_link_credit(sender.link) > 0 and not sender.closed():
    #         msg = pendingSends.pop(0)
    #         sender.log("sending %s" % msg)
    #         d = pn_delivery(sender.link, "post-delivery-%s" % len(pendingSends))
    #         rc = pn_link_send(sender.link, msg)
    #         if (rc < 0):
    #             print("Error: sending message: %s" % rc)
    #             return -2
    #         assert rc == len(msg)
    #         pn_link_advance(sender.link)  # deliver the message

    #     # settle any deliveries that the server has accepted
    #     sender.settle()

    # def settle(self):
    #     """ In order to be sure that the remote has accepted the message, we
    #     need to wait until the message's delivery has been remotely settled.
    #     Once that occurs, we can release the delivery by settling it.
    #     """
    #     d = pn_unsettled_head(self.link)
    #     while d:
    #         _next = pn_unsettled_next(d)
    #         # if the remote has either settled this delivery OR set the
    #         # disposition, we consider the message received.
    #         disp = pn_delivery_remote_state(d)
    #         if disp and disp != PN_ACCEPTED:
    #             print("Warning: message was not accepted by the remote!")
    #         if disp or pn_delivery_settled(d):
    #             pn_delivery_settle(d)
    #         d = _next


    # # done sending, now block until any pending deliveries are settled
    # sender.log("Done sending messages, waiting for deliveries to settle...");
    # while pn_link_unsettled(sender.link) > 0 and not sender.closed():
    #     sender.wait()
    #     sender.settle()



    # print "Sending to %s, server=%s exchange=%s namespace=%s fanout=%s" % (
    #     topic, opts.server, opts.exchange, opts.namespace, str(opts.fanout))

    # # teardown
    # for conn in conns:
    #     pn_connection_close(conn)
    #     # now wait for the connector to close
    #     while not pn_connector_closed(self.cxtr):
    #         self.wait()

    # for link in links:
    #     pn_link_free(link);

    # for ssn in sessions:
    #     pn_session_free(ssn);

    # for conn in conns:
    #     pn_connection_free(conn);
    #     pn_connector_free(cxtr);



if __name__ == "__main__":
    sys.exit(main())

