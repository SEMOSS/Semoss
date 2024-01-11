import logging
import sys

class CustomLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = kwargs.get("extra", {})
        stack = extra.get('stack', 'UNKNOWN') # maintain a default value for stack in case it's not given
        info = f"{stack} - "
        return '%s%s' % (info, msg), kwargs
    
cfg_log_format = f'CFG_PYTHON_LOGGER - %(levelname)s - %(message)s - %(name)s:%(lineno)d '

def get_logger(name):    
    logger = logging.getLogger(name)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(cfg_log_format))
    logger.addHandler(handler)
    logger.propagate = False

    return CustomLoggerAdapter(logger, {})
