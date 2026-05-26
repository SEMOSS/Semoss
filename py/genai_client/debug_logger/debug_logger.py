import logging, os


class DebugLogger:
    """
    Use in any of the clients like so:
    from ...debug_logger.debug_logger import DebugLogger

    logger = DebugLogger(
        log_dir="/Users/rweiler/Desktop/LOG_FILES",
        log_file_name="semoss_message_builder.txt",
        class_name=__name__,
    ).logger
    logger.info(json.dumps(input_messages, indent=2))
    """

    def __init__(self, log_dir: str, log_file_name: str, class_name: str):
        logger = logging.getLogger(class_name)
        logger.setLevel(logging.DEBUG)

        os.makedirs(log_dir, exist_ok=True)

        _file_handler = logging.FileHandler(
            os.path.join(log_dir, log_file_name), encoding="utf-8"
        )
        _file_handler.setLevel(logging.DEBUG)

        _formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s ::: %(message)s"
        )
        _file_handler.setFormatter(_formatter)

        logger.addHandler(_file_handler)

        self.logger = logger
