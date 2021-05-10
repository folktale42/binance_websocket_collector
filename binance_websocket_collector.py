#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import sys
import time
import os
import asyncio
import concurrent.futures

from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager
import websockets
from nifi_websocket import WebSocketClient


logging.basicConfig(level=logging.INFO)
API_MANAGERS = {
    'futures': BinanceWebSocketApiManager(
        exchange="binance.com-futures"
    )
}


class StreamsParameters:
    PRIVATE = [
        (
            API_MANAGERS['futures'],
            'arr',
            '!userData',
            'binance_futures_user_data',
        )
    ]
    PUBLIC = [
        (
            API_MANAGERS['futures'],
            'trade',
            'btcusdt',
            'binance_futures_trade_btcusdt',
        )
    ]


class BinanceApiCredentials:
    API_KEY = os.environ.get("BINANCE_API_KEY")
    API_SECRET = os.environ.get("BINANCE_API_SECRET")


class NifiWebSocketCredentials:
    USER = os.environ.get("NIFI_USER")
    PASSWORD = os.environ.get("NIFI_PASSWORD")


class NifiWebSocketParameters:
    BASE_URL = os.environ.get("NIFI_BASE_URL")
    DEFAULT_PATH = os.environ.get("NIFI_DEFAULT_PATH")
    PORT = int(os.environ.get("NIFI_DEFAULT_PORT"))


def process_binance_stream(
    binance_ws_api_manager: BinanceWebSocketApiManager,
    stream_id: str,
    websocket_client: WebSocketClient
):
    """
    Process a streams buffered events and dispatches them.

    :param ws_api_manager: Manages the websocket API.
    :param stream_id: The id of the websocket stream to process.
    """
    async def run():
        async with websocket_client as ws:
            count = 0
            count_k = 0

            while True:
                count = count + 1

                if count % 1000 == 0:
                    count_k = count_k + 1
                    count = 0
                    logging.info(
                        f"Stream id {stream_id} processing loop iteration #{count_k}k."
                    )
                    logging.info(
                        f"Current connection state: {ws.state} (CONNECTING=0, OPEN=1, CLOSING=2, CLOSED=3)."
                    )

                if binance_ws_api_manager.is_manager_stopping():
                    break

                oldest_event = binance_ws_api_manager.pop_stream_data_from_stream_buffer(
                    stream_buffer_name=stream_id
                )

                if not oldest_event:
                    time.sleep(0.01)
                    continue
                else:
                    try:
                        await ws.send(oldest_event)
                        logging.debug(f"Sent event: {oldest_event}")
                    except websockets.exceptions.ConnectionClosedError as cce:
                        logging.error(
                            f"ConnectionClosedError while awaiting on Nifi WebSocket send due to:{cce}."
                        )
                    except websockets.exceptions.WebSocketException as wse:
                        logging.warning(
                            f"WebSocketException while awaiting on Nifi WebSocket send due to: {wse}"
                        )
                    except BaseException as be:
                        logging.warning(
                            f"Exception while awaiting on Nifi WebSocket send due to: {be}"
                        )

            logging.warn(f"Ya getting out of the stream processing loop.")
    asyncio.run(run())
    logging.warn(f"Exiting stream id {stream_id}'s processing thread.")


def create_binance_ws_stream(api_manager: BinanceWebSocketApiManager, channels: str, markets: str, stream_label: str, api_key=False, api_secret=False, symbols=False, **kwargs):
    """
    Create a new Binance websocket stream and return the stream id.
    """
    stream_id = api_manager.create_stream(
        channels,
        markets,
        symbols=symbols,
        stream_label=stream_label,
        stream_buffer_name=True,
        api_key=api_key,
        api_secret=api_secret,
        ** kwargs
    )

    if not stream_id:
        raise Exception(f"Failed to create stream {stream_label}.")

    logging.info(
        f"Stream {stream_label} created. Channels: {channels}. Markets: {markets}. Symbols {symbols}. Stream id: {stream_id}."
    )
    return stream_id


def stop_managers(threads: list):
    """
    Stops all managers and all streams.
    """
    for k, manager in API_MANAGERS.items():
        logging.info(f"Stopping manager {k} and all it's streams.")
        manager.stop_manager_with_all_streams()


def get_ws_client():
    return WebSocketClient(
        NifiWebSocketParameters.BASE_URL,
        NifiWebSocketParameters.DEFAULT_PATH,
        NifiWebSocketParameters.PORT,
        NifiWebSocketCredentials.USER,
        NifiWebSocketCredentials.PASSWORD
    )


def main():
    try:
        streams = dict()

        for stream_params_tuple in StreamsParameters.PRIVATE:
            streams[
                create_binance_ws_stream(
                    *stream_params_tuple,
                    api_key=BinanceApiCredentials.API_KEY,
                    api_secret=BinanceApiCredentials.API_SECRET,
                )
            ] = (
                stream_params_tuple[3],
                stream_params_tuple[0],
                get_ws_client(),
            )  # { stream_id: (stream_label, binance_ws_api_manager, websocket_client), ... }

        for stream_params_tuple in StreamsParameters.PUBLIC:
            streams[
                create_binance_ws_stream(
                    *stream_params_tuple
                )
            ] = (
                stream_params_tuple[3],
                stream_params_tuple[0],
                get_ws_client(),
            )  # { stream_id: (stream_label, binance_ws_api_manager, websocket_client), ... }
    except BaseException as be:
        logging.error(f"Failed to create streams due to: {be}.")
        sys.exit(1)

    with concurrent.futures.ThreadPoolExecutor() as pool:
        results = [
            pool.submit(
                process_binance_stream,
                v[1],  # Api Manager
                k,     # Stream Id
                v[2]   # WebSocket Client
            ) for k, v in streams.items()
        ]

        for result in results:
            logging.info(f'Custom thread pool {str(result)}')

        try:
            while True:
                for manager_name, manager in API_MANAGERS.items():
                    manager.print_summary(
                        f"\n######## {manager_name.capitalize()} Api Manager Summary ########"
                    )
                time.sleep(5)
        finally:
            logging.info("Exiting...")
            stop_managers()


if __name__ == "__main__":
    logging.info("Starting Binance websocket data collector.")
    main()
