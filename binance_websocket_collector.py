#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import concurrent.futures
import logging
import os
import sys
import time

from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import \
    BinanceWebSocketApiManager

from nifi_websocket import NifiWebSocketClient

logging.basicConfig(level=logging.INFO)

BINANCE_WS_API_MANAGERS = {
    'futures': BinanceWebSocketApiManager(
        exchange="binance.com-futures"
    )
}


class StreamsParameters:
    PRIVATE = [
        (
            BINANCE_WS_API_MANAGERS['futures'],
            'arr',
            '!userData',
            'binance_futures_user_data',
        )
    ]
    PUBLIC = [
        (
            BINANCE_WS_API_MANAGERS['futures'],
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


class BinanceStreamClosedException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class CreateBinanceStreamFailedException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


def get_oldest_event(binance_ws_api_manager: BinanceWebSocketApiManager, stream_id: str) -> str or dict or False:
    """
    Returns the oldest event in a Binance WS Api Manager stream's buffer.
    """
    return binance_ws_api_manager.pop_stream_data_from_stream_buffer(
        stream_buffer_name=stream_id
    )


def get_stream_buffer_length(binance_ws_api_manager: BinanceWebSocketApiManager, stream_id: str) -> int:
    """
    Returns the length of a Binance WS stream's buffer array.
    """
    return len(binance_ws_api_manager.stream_buffers[stream_id])


def process_binance_stream(
    binance_ws_api_manager: BinanceWebSocketApiManager,
    stream_id: str,
    nifi_websocket_client: NifiWebSocketClient
):
    """
    Retrieves events from the stream's buffer and dispatches them to Apache Nifi.
    """
    async def run():
        # To preserve resources wait until there is data in the stream buffer before connecting.
        while get_stream_buffer_length(binance_ws_api_manager, stream_id) == 0:
            await asyncio.sleep(0.025)

        logging.info(
            f"Starting stream processing loop for stream {stream_id}."
        )

        async with nifi_websocket_client as ws:
            count = 0
            count_k = 0

            while ws.open:
                count = count + 1

                if count % 1000 == 0:
                    count_k = count_k + 1
                    count = 0
                    logging.info(
                        f"Stream {stream_id} processing loop iteration #{count_k}k."
                    )

                if binance_ws_api_manager.is_manager_stopping():
                    raise BinanceStreamClosedException(
                        f"Stream {stream_id} has stopped."
                    )

                oldest_event = get_oldest_event(
                    binance_ws_api_manager,
                    stream_id
                )

                if not oldest_event:
                    await asyncio.sleep(0.01)
                    continue
                else:
                    await ws.send(oldest_event)
                    logging.debug(
                        f"Stream {stream_id}'s processing loop sent event to Apache Nifi. Event: {oldest_event}."
                    )

                    # due to https://github.com/aaugustin/websockets/issues/84
                    await asyncio.sleep(0)
            logging.warning(
                f"Connection with Apache Nifi closed. Exiting stream {stream_id}'s processing loop."
            )
    while True:
        try:
            asyncio.run(run())
        except BinanceStreamClosedException:
            logging.info(
                f"Binance stream {stream_id} closed. Exiting processing loop."
            )
            break
        except BaseException as be:
            logging.warning(
                f"Exception while processing stream {stream_id}. Cause: {be}."
            )
        logging.info(
            f"Retrying processing loop for Binance stream {stream_id} in 1 second."
        )
        time.sleep(1)


def create_binance_ws_stream(
        api_manager: BinanceWebSocketApiManager,
        channels: str,
        markets: str,
        stream_label: str,
        api_key=False,
        api_secret=False,
        symbols=False,
        **kwargs
):
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
        raise CreateBinanceStreamFailedException(
            f"Failed to create stream {stream_label}."
        )

    logging.info(
        f"Stream {stream_label} created. Channels: {channels}. Markets: {markets}. Symbols: {symbols}. Stream id: {stream_id}."
    )
    return stream_id


def stop_binance_ws_api_managers():
    """
    Stops all Binance API managers.
    """
    for k, manager in BINANCE_WS_API_MANAGERS.items():
        logging.info(f"Stopping manager {k} and all it's streams.")
        manager.stop_manager_with_all_streams()


def get_nifi_ws_client():
    """
    Returns a NifiWebSocketClient instance with environment parameters.
    """
    return NifiWebSocketClient(
        NifiWebSocketParameters.BASE_URL,
        NifiWebSocketParameters.DEFAULT_PATH,
        NifiWebSocketParameters.PORT,
        NifiWebSocketCredentials.USER,
        NifiWebSocketCredentials.PASSWORD
    )


def is_all_done(results):
    """
    Returns a boolean indicating whether all results have status done.
    """
    done = 0
    for r in results:
        if r.done():
            done += 1

    return done == len(results)


def main():
    try:
        # streams = { stream_id: (stream_label, binance_ws_api_manager, websocket_client), ... }
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
                get_nifi_ws_client(),
            )

        for stream_params_tuple in StreamsParameters.PUBLIC:
            streams[
                create_binance_ws_stream(
                    *stream_params_tuple
                )
            ] = (
                stream_params_tuple[3],
                stream_params_tuple[0],
                get_nifi_ws_client(),
            )
    except CreateBinanceStreamFailedException as csfe:
        logging.error(f"Failed to create streams due to: {csfe}.")
        print(sys.exc_info()[2])
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

        try:
            while True:
                time.sleep(5)  # sleep while threads spawn

                if is_all_done(results):
                    logging.info("All stream processing threads are done.")
                    break

                for manager_name, manager in BINANCE_WS_API_MANAGERS.items():
                    manager.print_summary(
                        f"\n######## {manager_name.capitalize()} Api Manager Summary ########"
                    )
                    logging.info(
                        f"{manager_name.capitalize()} Api Manager stream buffer length: {manager.get_stream_buffer_length()}."
                    )
                    logging.info(
                        f"{manager_name.capitalize()} Api Manager endpoint errors: {manager.get_errors_from_endpoints()}."
                    )
        finally:
            logging.info("Exiting...")
            stop_binance_ws_api_managers()


if __name__ == "__main__":
    logging.info("Starting Binance websocket data collector.")
    main()
