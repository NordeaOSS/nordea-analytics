def log_to_file() -> None:
    """Write logs to APPDATA folder ."""
    import datetime
    import http.client
    import logging
    import os

    appdata = os.getenv("APPDATA") or "."

    http.client.HTTPConnection.debuglevel = 1

    logging.basicConfig(
        filename=appdata
        + "\\analytics-api.{:%Y-%m-%d.%H-%M}.log".format(datetime.datetime.now()),
        filemode="a",
        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG,
    )
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True
