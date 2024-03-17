from __future__ import annotations
from typing import (
    Generator,
    Set,
    List,
    Optional,
    NoReturn,
)
import os, sys, contextlib, queue, asyncio, logging, websockets, websockets.server, signal, platform, orjson, mmap, struct
from websockets.legacy.server import WebSocketServer, WebSocketServerProtocol
from time import sleep
from pathlib import Path
from threading import Thread
from websockets.exceptions import (
    WebSocketException,
    ConnectionClosed,
    ConnectionClosedOK,
    ConnectionClosedError,
)
from datetime import datetime as dtdt
from logging.handlers import RotatingFileHandler

if sys.platform.startswith("win"):
    from signal import SIGABRT, SIGINT, SIGTERM

    SIGNALS = (SIGABRT, SIGINT, SIGTERM)
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
else:
    try:
        import uvloop
    except (ImportError, ModuleNotFoundError):
        os.system(f"{sys.executable} -m pip install uvloop")
        import uvloop

    from signal import SIGABRT, SIGINT, SIGTERM, SIGHUP

    SIGNALS = (SIGABRT, SIGINT, SIGTERM, SIGHUP)
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

__all__ = ["ReplayMarketData"]


class ProgramKilled(Exception):
    """ProgramKilled Checks the ProgramKilled exception"""

    pass  # type: ignore


class ReplayMarketData:
    LOGGING_FORMAT: str = "[%(levelname)s]|[%(asctime)s]|[%(name)s::%(module)s::%(funcName)s::%(lineno)d]|=> %(message)s"
    PING_INTERVAL: float = 2.5
    KEEPALIVE_INTERVAL: int = 5
    MAXDELAY: int = 5
    MAXRETRIES: int = 10
    CONNECT_TIMEOUT: int = 30
    RECONNECT_MAX_DELAY: int = 60
    RECONNECT_MAX_TRIES: int = 50
    CLOSE_TIMEOUT: int = 30
    MODE_FULL: str = "full"
    MODE_QUOTE: str = "quote"
    MODE_LTP: str = "ltp"
    SUBSCRIBE: str = "SUBSCRIBE"
    UNSUBSCRIBE: str = "UNSUBSCRIBE"
    SETMODE: str = "SETMODE"
    MESSAGE_CODE = 11
    MESSAGE_SUBSCRIBE = "subscribe"
    MESSAGE_UNSUBSCRIBE = "unsubscribe"
    MESSAGE_SETMODE = "mode"
    TICK_UPDATE: str = "TICK_UPDATE"
    ORDER_UPDATE: str = "ORDER_UPDATE"
    ERROR_UPDATE: str = "ERROR_UPDATE"
    MESSAGE_UPDATE: str = "MESSAGE_UPDATE"
    MINIMUM_RECONNECT_MAX_DELAY: int = 5
    MAXIMUM_RECONNECT_MAX_TRIES: int = 300
    PING_PAYLOAD: str = ""
    ON_TICKS: str = "ON_TICKS"
    ON_EXTENDED_TICKS: str = "ON_EXTENDED_TICKS"
    ON_ORDER_UPDATES: str = "ON_ORDER_UPDATES"

    @staticmethod
    def get_now_date_time_with_microseconds_string() -> str:
        return dtdt.now().strftime("%d_%b_%Y_%H_%M_%S_%f")

    @staticmethod
    def is_windows() -> bool:
        return (
            os.name == "nt"
            and sys.platform == "win32"
            and platform.system() == "Windows"
        )

    @staticmethod
    def is_linux() -> bool:
        return (
            os.name == "posix"
            and platform.system() == "Linux"
            and sys.platform in {"linux", "linux2"}
        )

    @staticmethod
    def is_mac() -> bool:
        return (
            os.name == "posix"
            and sys.platform == "darwin"
            and platform.system() == "Darwin"
        )

    @staticmethod
    def get_logger(name, filename, level=logging.WARNING) -> logging.Logger:
        logger = logging.getLogger(name)
        logger.setLevel(level)

        stream = logging.StreamHandler()
        stream.setFormatter(logging.Formatter(ReplayMarketData.LOGGING_FORMAT))
        logger.addHandler(stream)

        fh = RotatingFileHandler(filename, maxBytes=100 * 1024 * 1024, backupCount=25)
        fh.setFormatter(logging.Formatter(ReplayMarketData.LOGGING_FORMAT))
        logger.addHandler(fh)
        logger.propagate = False
        return logger

    @staticmethod
    def start_background_loop(loop: asyncio.AbstractEventLoop) -> Optional[NoReturn]:
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        except (KeyboardInterrupt, SystemExit):
            loop.run_until_complete(loop.shutdown_asyncgens())
            if loop.is_running():
                loop.stop()
            if not loop.is_closed():
                loop.close()

    def __aenter__(self) -> "ReplayMarketData":
        return self

    def __enter__(self) -> "ReplayMarketData":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.__graceful_exit()

    def __del__(self) -> None:
        self.__graceful_exit()

    def __delete__(self) -> None:
        self.__graceful_exit()

    def __graceful_exit(self) -> None:
        with contextlib.suppress(RuntimeError, RuntimeWarning):
            self.stop()
            asyncio.run_coroutine_threadsafe(
                self.__loop.shutdown_asyncgens(), self.__loop
            ).result(5.0)
            if self.__loop.is_running():
                self.__loop.stop()
            if not self.__loop.is_closed():
                self.__loop.close()

    def handle_stop_signals(self, *args, **kwargs):
        try:
            self.__graceful_exit()
        except Exception as err:
            self.log.error(str(err))
        else:
            exit()

    def __initialize_loop(self) -> None:
        self.__loop = asyncio.new_event_loop()
        if ReplayMarketData.is_windows():
            with contextlib.suppress(ValueError):
                for sig in SIGNALS:
                    signal.signal(sig, self.handle_stop_signals)
        else:
            with contextlib.suppress(ValueError):
                for sig in SIGNALS:
                    self.__loop.add_signal_handler(sig, self.handle_stop_signals)
        self._event_thread = Thread(
            target=self.start_background_loop,
            args=(self.__loop,),
            name=f"{self.__class__.__name__}_event_thread",
            daemon=True,
        )
        self._event_thread.start()
        self.log.info("ReplayMarketData Event Loop has been initialized.")

    def __init__(
        self,
        debug: bool = False,
        debug_verbose: bool = False,
        tick_frequency_in_milliseconds: float = 1.0,
    ) -> None:
        self.clients: Set[WebSocketServerProtocol] = set()
        self.first_client: Optional[WebSocketServerProtocol] = None
        self._datas_to_be_replayed: List[Path] = []
        self.debug: bool = debug
        self.debug_verbose: bool = debug_verbose
        self.delay_bw_ticks: float = tick_frequency_in_milliseconds / 1000.0
        logging.getLogger("websockets").addHandler(logging.NullHandler())
        if self.debug or self.debug_verbose:
            self.log_level = (
                logging.INFO
                if self.debug
                else logging.DEBUG
                if self.debug_verbose
                else logging.WARNING
            )
            self.logfile = Path.cwd().joinpath(
                "logs/replay_market_data_"
                + f"{ReplayMarketData.get_now_date_time_with_microseconds_string()}.log"
            )
            os.makedirs(self.logfile.parent, exist_ok=True)
            self.log = ReplayMarketData.get_logger(
                "NseFetch", filename=self.logfile, level=self.log_level
            )
            logging.basicConfig(
                format=ReplayMarketData.LOGGING_FORMAT, level=self.log_level
            )
            if self.debug:
                logging.getLogger("websockets").propagate = False
                logging.getLogger("websockets").setLevel(logging.WARNING)
            if self.debug_verbose:
                logging.getLogger("websockets").propagate = self.debug_verbose
                logging.getLogger("websockets").setLevel(self.log_level)
        self.ticker_feed_running: bool = False
        self.should_run: bool = True
        self.stop_stream_queue: queue.Queue = queue.Queue()
        self.tick_data_folder = Path.cwd().joinpath("tick_datas")
        if not (self.tick_data_folder.exists() and self.tick_data_folder.is_dir()):
            self.log.debug(
                "Tick Datas Folder Does Not Exist, Creating Now, "
                "Ensure To Place The Tick Data Files Inside `tick_datas` Folder, "
                "The Restart The Program"
            )
            os.makedirs(self.tick_data_folder, exist_ok=True)
            exit(0)
        self._start_replaying: bool = False
        self.call_for_exit: bool = False
        self.__initialize_loop()
        self.run()

    async def __ws_handler(self, websocket: WebSocketServerProtocol) -> None:
        if len(self.clients) == 0 and self.first_client is None:
            self.first_client = websocket
        self.clients.add(websocket)
        try:
            await websocket.wait_closed()
        finally:
            if len(self.clients) == 1 and self.first_client is not None:
                self.first_client = None
            self.clients.remove(websocket)

    async def __get_ws_server(self) -> WebSocketServer:
        return await websockets.server.serve(
            self.__ws_handler,
            host="localhost",
            port=8765,
            logger=self.log,
            select_subprotocol=None,
            ping_interval=self.PING_INTERVAL,
            ping_timeout=self.KEEPALIVE_INTERVAL,
            close_timeout=self.CLOSE_TIMEOUT,
            # start_serving=False,
        )

    def run(self) -> None:
        try:
            if self.__loop.is_running():
                self.run_future = asyncio.run_coroutine_threadsafe(
                    self.__run_forever(), self.__loop
                )
        except KeyboardInterrupt:
            self.log.info("Keyboard Interrupt Detected, Bye...")

    def stop(self) -> None:
        if self.__loop.is_running():
            asyncio.run_coroutine_threadsafe(
                self.__stop_ws(),
                self.__loop,
            ).result(7.5)
            if self.run_future.running():
                self.run_future.cancel()
                while not self.run_future.done():
                    sleep(0.025)

    async def __stop_ws(self) -> None:
        self.should_run = False
        if self.stop_stream_queue.empty():
            self.stop_stream_queue.put_nowait({"should_stop": True})
        await asyncio.sleep(1.0)
        await self.__close()

    async def __close(self) -> None:
        if self.ticker_feed is not None and self.ticker_feed.is_serving():
            self.log.info("Initiating Websocket Server Closing Procedures.")
            self.log.info("Calling websocket.close() task.")
            self.ticker_feed.close()
            try:
                async with asyncio.timeout(2.5):
                    await self.ticker_feed.wait_closed()
            except TimeoutError:
                self.log.critical(
                    "Websocket Closing Task Timed Out After Waiting For 2.5 Seconds"
                )
            except Exception as err:
                self.log.error("Websocket Closing Task Encountered An Error: %s", err)
            else:
                self.log.info("Calling websocket.close() completed succesfully.")
            self.ticker_feed_running, self.ticker_feed = False, None

    async def __connect(self) -> None:
        self.log.info("Initialzing Websocket Server: %s", "ws://localhost:8765")
        self.ticker_feed = await self.__get_ws_server()
        await asyncio.sleep(0.01)
        if (
            self.ticker_feed is not None
            and isinstance(self.ticker_feed, WebSocketServer)
            and self.ticker_feed.is_serving()
        ):
            self.ticker_feed_running = True
            self.log.info(
                "Websocket Server Up And Running SUccessfully At: %s",
                "ws://localhost:8765",
            )

    async def __run_forever(self) -> None:
        await asyncio.sleep(0.1)
        reconnect_sleep_time: float = 0.5
        while True:
            if not self.stop_stream_queue.empty() or not self.should_run:
                # self.stop_stream_queue.get(timeout=1.0)
                self.log.info("Ticker Feeds Stopped Successfully.")
                return
            if not self.ticker_feed_running:
                self.log.info(
                    "Initializing Or Restarting Ticker Feed Websocket Stream."
                )
                await self.__connect()
            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self.__consume())
                    tg.create_task(self.__dispatch())
            except* WebSocketException as wse:
                self.log.warning(
                    "Ticker Feeds Websocket Server Has Met With An Exception, During Its Communication With Clients, Exception Was: %s",
                    wse,
                )
                await self.__close()
                await asyncio.sleep(reconnect_sleep_time)
                reconnect_sleep_time += 0.05
                if self.first_client is None:
                    await self.__stop_ws()
                    self.call_for_exit = True
            except* Exception as exc:
                self.log.critical(
                    "An Exception Has Occured While Consuming And Dispatching The Stream From & To Ticker Feed, Exception Was: %s",
                    exc,
                )
                if self.first_client is None:
                    await self.__stop_ws()
                    self.call_for_exit = True
            await asyncio.sleep(0.001)

    async def __consume(self) -> None:
        await asyncio.sleep(0.05)
        while True:
            data = None
            if not self.stop_stream_queue.empty() or not self.should_run:
                self.log.info("Ticker Feeds Consume Task Stopped Successfully.")
                return
            if not self.ticker_feed_running:
                self.log.info(
                    "Ticker Feeds Not Running, As A Result, Consume Task Stopped Successfully."
                )
                return
            if self.first_client is not None and not self._start_replaying:
                try:
                    async with asyncio.timeout(1.0):
                        if self.ticker_feed is not None:
                            try:
                                data = await self.first_client.recv()
                            except (
                                ConnectionClosed,
                                ConnectionClosedOK,
                                ConnectionClosedError,
                            ):
                                self.ticker_feed_running = False
                                self._start_replaying = False
                                self._datas_to_be_replayed = []
                                raise WebSocketException(
                                    "Master Client Websocket Connection Was Found To Be Closed, Hence Can't Consume And Dispatch Any Data."
                                )
                            except (RuntimeError, RuntimeWarning):
                                continue
                except TimeoutError:
                    continue
                except (RuntimeError, RuntimeWarning):
                    continue
                else:
                    try:
                        if data is not None and isinstance(data, (str, bytes)):
                            data = orjson.loads(data)
                            if (
                                "a" in data
                                and data.get("a") is not None
                                and isinstance(data["a"], str)
                                and data["a"] == ReplayMarketData.MESSAGE_SUBSCRIBE
                                and "v" in data
                                and data.get("v") is not None
                                and isinstance(data["v"], list)
                                and len(data["v"]) > 0
                                and all(
                                    [
                                        isinstance(d, int) and len(str(d)) == 8
                                        for d in data["v"]
                                    ]
                                )
                            ):
                                self._datas_to_be_replayed.extend(
                                    [
                                        _tick_data_file
                                        for _tick_data_file in [
                                            self.tick_data_folder.joinpath(
                                                f"tick_data_{d[:4]}-{d[4:6]}-{d[6:]}.bin"
                                            )
                                            for d in [str(d) for d in data["v"]]
                                        ]
                                        if _tick_data_file.exists()
                                        and _tick_data_file.is_file()
                                    ]
                                )
                                if (
                                    len(self._datas_to_be_replayed) > 0
                                    and not self._start_replaying
                                ):
                                    self.log.info(
                                        "Proceeding With The Replay Of These Tick Data Files: %s",
                                        self._datas_to_be_replayed,
                                    )
                                    self._start_replaying = True
                                    await asyncio.sleep(1.0)
                    except TimeoutError:
                        continue
                    except Exception as exc:
                        self.log.exception(exc, exc_info=True)
                        continue
            await asyncio.sleep(0.001)

    async def __dispatch(self) -> None:
        await asyncio.sleep(0.1)
        while True:
            if not self.stop_stream_queue.empty() or not self.should_run:
                self.log.info("Ticker Feeds Dispatch Task Stopped Successfully.")
                return
            if not self.ticker_feed_running:
                self.log.info(
                    "Ticker Feeds Not Running, As A Result, Dispatch Task Stopped Successfully."
                )
                return
            if self._start_replaying and len(self._datas_to_be_replayed) > 0:
                self._start_replaying = False
                for file in self._datas_to_be_replayed:
                    for data in self.read_binary_file(file):
                        try:
                            websockets.broadcast(self.clients, data)
                        except TimeoutError:
                            pass
                        except (RuntimeError, RuntimeWarning):
                            pass
                        except Exception as exc:
                            self.log.exception(exc, exc_info=True)
                        await asyncio.sleep(self.delay_bw_ticks)
                    await asyncio.sleep(self.delay_bw_ticks)
                self._datas_to_be_replayed = []
            await asyncio.sleep(0.001)

    def read_binary_file(self, file: Path) -> Generator[bytes, Any, None]:
        with open(file, "rb") as f:
            mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            length_bytes = mmapped_file.read(4)
            while length_bytes:
                length = struct.unpack(">I", length_bytes)[0]
                try:
                    data = mmapped_file.read(length)
                except ValueError as err:
                    self.log.error("Reached End Of The File: %s, Error: %s", file, err)
                else:
                    yield bytes(data)
                finally:
                    length_bytes = mmapped_file.read(4)

            self.log.error("Expected 4 bytes for length, got: %d", len(length_bytes))
            self.log.info("It Appears We Have Reached The End Of File: %s", file)
            mmapped_file.close()
