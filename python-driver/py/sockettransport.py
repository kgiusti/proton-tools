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

import socket, errno, time, heapq, select
from proton import Transport


class SocketTransport(object):
    """Provides network I/O for a Proton Transport via a socket.
    """
    def __init__(self, socket, name=None):
        """socket - Python socket. Expected to be configured and connected.
        name - optional name for this SocketTransport
        """
        self._name = name
        self._socket = socket
        self._transport = Transport()
        self._read_done = False
        self._write_done = False
        self._next_tick = 0

    def fileno(self):
        """Allows use of a SocketTransport by the select module.
        """
        return self._socket.fileno()

    @property
    def transport(self):
        return self._transport

    @property
    def socket(self):
        return self._socket

    @property
    def name(self):
        return self._name

    def tick(self, now):
        """Invoke the transport's tick method.  'now' is seconds since Epoch.
        Returns the timestamp for the next tick, or zero if no next tick.
        """
        self._next_tick = self._transport.tick(now)
        return self._next_tick

    @property
    def next_tick(self):
        """Timestamp for next call to tick()
        """
        return self._next_tick

    @property
    def need_read(self):
        """True when the Transport requires data from the network layer.
        """
        return (not self._read_done) and self._transport.capacity() > 0

    @property
    def need_write(self):
        """True when the Transport has data to write to the network layer.
        """
        return (not self._write_done) and self._transport.pending() > 0

    def read_input(self):
        """Read from the network layer and processes all data read.  Can
        support both blocking and non-blocking sockets.
        """
        if self._read_done:
            return None

        c = self._transport.capacity()
        if c < 0:
            try:
                self._socket.shutdown(socket.SHUT_RD)
            except:
                pass
            self._read_done = True
            return None

        if c > 0:
            try:
                buf = self._socket.recv(c)
                if buf:
                    self._transport.push(buf)
                    return len(buf)
                # else socket closed
                self._transport.close_tail()
                self._read_done = True
                return None
            except socket.timeout, e:
                raise  # let the caller handle this
            except socket.error, e:
                err = e.args[0]
                if (err != errno.EAGAIN and
                    err != errno.EWOULDBLOCK and
                    err != errno.EINTR):
                    # otherwise, unrecoverable:
                    self._transport.close_tail()
                    self._read_done = True
                raise
            except:  # beats me...
                self._transport.close_tail()
                self._read_done = True
                raise
        return 0

    def write_output(self):
        """Write data to the network layer.  Can support both blocking and
        non-blocking sockets.
        """
        # @todo - KAG: need to explicitly document error return codes/exceptions!!!
        if self._write_done:
            return None

        c = self._transport.pending()
        if c < 0:  # output done
            try:
                self._socket.shutdown(socket.SHUT_WR)
            except:
                pass
            self._write_done = True
            return None

        if c > 0:
            buf = self._transport.peek(c)
            try:
                rc = self._socket.send(buf)
                if rc > 0:
                    self._transport.pop(rc)
                    return rc
                # else socket closed
                self._transport.close_head()
                self._write_done = True
                return None
            except socket.timeout, e:
                raise # let the caller handle this
            except socket.error, e:
                err = e.args[0]
                if (err != errno.EAGAIN and
                    err != errno.EWOULDBLOCK and
                    err != errno.EINTR):
                    # otherwise, unrecoverable
                    self._transport.close_head()
                    self._write_done = True
                raise
            except:
                self._transport.close_tail()
                self._write_done = True
                raise
        return 0

    @property
    def done(self):
        return self._write_done and self._read_done


class SocketTransports(object):
    """Container for SocketTransports.  Provides convenience methods for I/O
    and timer event processing.
    """

    def __init__(self):
        self._transports = {}
        self._timer_heap = [] # (next_tick, sockettransport)

    def __iter__(self):
        return self._transports.itervalues()

    def add(self, socket_transport):
        if not socket_transport.name:
            raise KeyError('SocketTransport not named')
        if socket_transport.name in self._transports:
            raise KeyError('SocketTransport name clash')
        self._transports[socket_transport.name] = socket_transport

    def remove(self, name):
        del self._transports[name]

    def need_io(self):
        """Return a pair of lists containing those socket-transports that are
        read-blocked and write-ready.
        """
        readfd = []
        writefd = []
        for st in self._transports.itervalues():
            if st.need_read:
                readfd.append(st)
            if st.need_write:
                writefd.append(st)
        return (readfd, writefd)

    def get_next_tick(self):
        """Returns the next pending timeout timestamp in seconds since Epoch,
        or 0 if no pending timeouts.  process_io() must be called at least
        once prior to the timeout.
        """
        if not self._timer_heap:
            return 0
        return self._timer_heap[0][0]

    def process_io(self, readable, writable):
        """Does all I/O and timer related processing.  readble is a sequence of
        sockettransports whose sockets are readable.  'writable' is a sequence
        of sockettransports whose sockets are writable. This method returns a
        set of all the SocketTransport in the container that have been
        processed (this includes the arguments plus any that had timers
        processed).  Use get_next_tick() to determine when process_io() must
        be invoked again.
        """

        work_list = set()
        for st in readable:
            st.read_input()
            # check if process_read set a timer
            if st.next_tick == 0:
                st.tick(time.time())
            if st.next_tick:
                heapq.heappush(self._timer_heap, (st.next_tick, st))
            work_list.add(st)

        # process any expired transport ticks
        while self._timer_heap and self._timer_heap[0][0] <= time.time():
            st = heapq.heappop(self._timer_heap)
            st.tick(time.time())
            if st.next_tick:   # timer reset
                heapq.heappush(self._timer_heap, (st.next_tick, st))
            work_list.add(st)

        for st in writable:
            st.write_output()
            # check if process_write set a timer
            if st.next_tick == 0:
                st.tick(time.time())
            if st.next_tick:
                heapq.heappush(self._timer_heap, (st.next_tick, st))
            work_list.add(st)

        return work_list


    def do_io(self, deadline=None):
        """Wait for I/O or timer events on all of the SocketTransports in the
        container.  deadline - seconds since Epoch, block waiting for I/O and
        timer events until either I/O or timers expire or this timestamp is
        reached.  If deadline is None, block indefinately until either I/O
        occurs or a timer expires.
        """

        readfd, writefd = self.need_io()

        timeout = None
        next_tick = self.get_next_tick()
        if next_tick:
            deadline = deadline if (deadline and deadline < next_tick) else next_tick
        if deadline:
            now = time.time()
            deadline = 0 if deadline <= now else now - deadline

        readable,writable,ignore = select.select(readfd,writefd,[],timeout)

        return self.process_io(readable, writable)


__all__ = [
    "SocketTransport",
    "SocketTransports"
    ]
