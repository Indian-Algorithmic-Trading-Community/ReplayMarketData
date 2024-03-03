from kiteconnect import KiteTicker

REPLAY_SERVER = "ws://localhost:8765"
dates = [20240301, 20240302]

def on_ticks(ws, ticks):
    print("Tick Update Recieved >>> ", ticks)


def on_connect(ws, response):
    ws.subscribe(dates)


def on_close(ws, code, reason):
    print("Connection closed: {code} - {reason}".format(code=code, reason=reason))


def on_error(ws, code, reason):
    print("Connection error: {code} - {reason}".format(code=code, reason=reason))


def on_reconnect(ws, attempts_count):
    print("Reconnecting: {}".format(attempts_count))


def on_noreconnect(ws):
    print("Reconnect failed.")


if __name__ == "__main__":
    kws = KiteTicker(api_key="", access_token="", root=REPLAY_SERVER)
    kws.on_ticks = on_ticks
    kws.on_close = on_close
    kws.on_error = on_error
    kws.on_connect = on_connect
    kws.on_reconnect = on_reconnect
    kws.on_noreconnect = on_noreconnect
    kws.connect()
