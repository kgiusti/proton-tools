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

from proton import Connection, Endpoint, SASL

class EndpointEventHandler(object):
    # @todo: why need active()?  Will it use excessive cpu?
    def sasl_done(self, sasl):
        pass

    def connection_pending(self, connection):
        connection.open()

    def connection_active(self, connection):
        pass

    def connection_remote_closed(self, connection):
        connection.close()

    def session_pending(self, session):
        session.open()

    def session_active(self, session):
        pass

    def session_remote_closed(self, session):
        session.close()

    def link_pending(self, link):
        link.open()

    def link_active(self, link):
        pass

    def link_closed(self, link):
        link.closed()

    def delivery_update(self, delivery):
        pass

_NEED_INIT = Endpoint.LOCAL_UNINIT
_NEED_CLOSE = (Endpoint.LOCAL_ACTIVE|Endpoint.REMOTE_CLOSED)

class Interconnect(object):
    # @todo - Need a better name than "Interconnect"
    def __init__(self, endpoint_handler, sasl):
        # @todo - remove sasl???
        self._connection = Connection()
        self._endpoint_cb = endpoint_handler
        self.sasl = sasl

    @property
    def connection(self):
        return self._connection

    def process_endpoints(self):

        # wait until SASL has authenticated
        # ??? Meh, why callback???
        if self.sasl:
            if self.sasl.state not in (SASL.STATE_PASS,
                                       SASL.STATE_FAIL):
                print("SASL wait.")
                return

            self._endpoint_cb.sasl_done(self.sasl)
            self.sasl = None

        if self._connection.state & _NEED_INIT:
            self._endpoint_cb.connection_pending(self._connection)

        ssn = self._connection.session_head(_NEED_INIT)
        while ssn:
            self._endpoint_cb.session_pending(ssn)
            ssn = ssn.next(_NEED_INIT)

        link = self._connection.link_head(_NEED_INIT)
        while link:
            self._endpoint_cb.link_pending(link)
            link = link.next(_NEED_INIT)

        # @todo: any need for ACTIVE callback?
        # process the work queue

        delivery = self._connection.work_head
        while delivery:
            self._endpoint_cb.delivery_update(delivery)
            delivery = delivery.work_next

        # close all endpoints closed by remotes

        link = self._connection.link_head(_NEED_CLOSE)
        while link:
            self._endpoint_cb.link_remote_closed(link)
            link = link.next(_NEED_CLOSE)

        ssn = self._connection.session_head(_NEED_CLOSE)
        while ssn:
            self._endpoint_cb.session_remote_closed(link)
            ssn = ssn.next(_NEED_CLOSE)

        if self._connection.state == (_NEED_CLOSE):
            self._endpoint_cb.connection_remote_closed(self._connection)


__all__ = [
    "EndpointEventHandler",
    "Interconnect"
    ]
