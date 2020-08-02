from io import StringIO
import logging
import datetime


class StreamsLogger:
    def __init__(self):
        self.logger = logging.getLogger(name=__name__)
        self.logger.setLevel(level=logging.INFO)

        # create a file handler that puts all these log messages to file
        today_str = datetime.date.today().strftime("%Y-%d-%m")
        fh = logging.FileHandler(filename=f"{today_str}_extras.log")
        fh.setLevel(level=logging.INFO)

        # console handler, something to print to console
        ch = logging.StreamHandler()
        ch.setLevel(level=logging.INFO)

        # create formatter, and add formatter to streamer
        formatter = logging.Formatter(
            fmt="%(asctime)s-{%(pathname)s:%(lineno)d}-%(levelname)s-%(message)s",
            datefmt="%Y-%m-%d %I:%M:%S %p",
        )

        # set all these formatters to the file handler and the stream (console) handler
        ch.setFormatter(fmt=formatter)
        fh.setFormatter(fmt=formatter)

        # add ch, fh to logger
        self.logger.addHandler(hdlr=ch)
        self.logger.addHandler(hdlr=fh)


class TqdmToLogger(StringIO):
    """
        Output stream for tqdm which will output to logger module instead of
        the StdOut.
    """

    logger = None
    level = None
    buf = ""

    def __init__(self, logger, level=None):
        super(TqdmToLogger, self).__init__()
        self.logger = logger
        self.level = level or logging.INFO

    def write(self, buf):
        self.buf = buf.strip("\r\n\t ")

    def flush(self):
        self.logger.log(self.level, self.buf)
