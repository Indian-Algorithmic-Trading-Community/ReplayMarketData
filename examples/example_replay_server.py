import signal
from time import sleep
from replaymarketdata import ReplayMarketData

rmd = ReplayMarketData(debug=True)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, rmd.handle_stop_signals)
    signal.signal(signal.SIGTERM, rmd.handle_stop_signals)
    while True:
        try:
            if rmd.call_for_exit:
                break
        except KeyboardInterrupt:
            break
        sleep(1)
    rmd.handle_stop_signals()
