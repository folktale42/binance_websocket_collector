#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from ssl import CERT_NONE, PROTOCOL_TLS_CLIENT, SSLContext

import websockets
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import WebSocketException
from websockets.headers import build_authorization_basic


class NifiWebSocketConnectionException(WebSocketException):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class NifiWebSocketClient(object):
    """
    Context Manager for connections to a listening WebSocket server on Apache Nifi.

    Connects using Basic Auth.
    Does not verify SSL server certificate.
    Does not send pings/expects pongs. 

    >>> import asyncio
    >>> wsconn = NifiWebSocketClient("", "", None, "", "")
    >>> async def run():
    ...     await (await wsconn.__aenter__()).send("test")
    >>> asyncio.run(run())
    """

    def __init__(self, base_url: str, path: str, port: int, user: str, password: str, **kwargs_to_conn) -> None:
        super().__init__()
        self.base_url = base_url
        self.path = path
        self.port = port
        self.user = user
        self.password = password
        self.other_conn_args = kwargs_to_conn
        self.ssl_context = 'wss' in self.base_url and SSLContext(
            PROTOCOL_TLS_CLIENT) or None
        self._conn = None

        if self.ssl_context:
            # Allow insecure TLS
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = CERT_NONE

    def _get_uri(self):
        return f"{self.base_url}:{self.port}/{self.path}"

    async def __aenter__(self) -> WebSocketClientProtocol:
        uri = self._get_uri()

        # Create connection with Basic Auth.
        self._conn = websockets.client.connect(
            uri=uri,
            ssl=self.ssl_context,
            extra_headers={
                "Authorization": build_authorization_basic(
                    self.user,
                    self.password
                )
            },
            ping_interval=None,         # do not send pings
            ping_timeout=None,          # do not expect pongs
            ** self.other_conn_args
        )

        try:
            _ws_client = await self._conn.__aenter__()
            logging.info(
                f'Connected to {uri}. State: {_ws_client.open and "Open" or "Not Open"}'
            )
        except websockets.exceptions.WebSocketException as we:
            logging.warning(
                f"Websockets lib exception while awaiting on NifiWebSocketClient.__aenter__(). Cause: {we}."
            )
            raise NifiWebSocketConnectionException(
                f"Failed to estabilish a connection with {uri}."
            )

        return _ws_client

    async def __aexit__(self, *args, **kwargs):
        try:
            await self._conn.__aexit__(*args, **kwargs)
            logging.info(f'Disconnected from {self._get_uri()}.')
        except websockets.exceptions.WebSocketException as we:
            logging.warning(
                f"Websockets lib exception while awaiting on NifiWebSocketClient.__aexit__(). Cause: {we}."
            )


if __name__ == '__main__':
    import doctest
    doctest.testmod()
