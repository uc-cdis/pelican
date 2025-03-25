import cdislogging
import os

logging = cdislogging.get_logger(
    __name__, log_level="debug" if os.environ.get("DEBUG") else "info"
)
