import logging
import sys


class SmssLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = kwargs.get("extra", {})
        stack = extra.get(
            "stack", "UNKNOWN"
        )  # maintain a default value for stack in case it's not given
        msg = f'"stack": "{stack}", "message": "{msg}"'
        return msg, kwargs


cfg_log_format = 'SMSS_PYTHON_LOGGER<==<>==>{%(message)s, "levelName": "%(levelname)s", "name": "%(name)s", "lineNumber":"%(lineno)d"}'


def get_logger(name):
    logger = logging.getLogger(name)

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(cfg_log_format))
    logger.addHandler(handler)
    logger.propagate = False

    return SmssLoggerAdapter(logger, {})
