import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import warnings
from pathlib import Path
from typing import Dict, Any
import os
import json
import yaml

class LoggingHandler:
    DEFAULT_CONFIG = {
        "global_level": "DEBUG",
        "file": "app.log",
        "max_size_mb": 2,
        "backup_count": 5,
        "file_level": "DEBUG",
        "console_level": "INFO",
        "use_time_rotation": False,
        "log_format": "%(asctime)s - %(levelname)s - [%(filename)s: Line %(lineno)d] - %(process)d - %(threadName)s - %(message)s",
        "log_folder": "logs"  # Default log folder
    }

    @staticmethod
    def setup_logging(config: Dict[str, Any] = None, config_file: str = None):
        script_dir = Path(__file__).resolve().parent
        base_folder = script_dir.parent  # Base folder one level up from the script's directory
        config_folder = base_folder / 'configs'
        log_folder = base_folder / 'logs'

        if config_file:
            config = LoggingHandler._load_config_from_file(config_file)
        if not config:
            config = LoggingHandler.DEFAULT_CONFIG
            # Create the default config file in the configs folder
            config_file_path = config_folder / 'logging_config.json'
            LoggingHandler._create_default_config_file(config_file_path)

        global_level = os.getenv('LOG_GLOBAL_LEVEL', config.get('global_level', 'DEBUG'))
        log_file = os.getenv('LOG_FILE', config.get('file', 'app.log'))
        log_max_size_mb = int(os.getenv('LOG_MAX_SIZE_MB', config.get('max_size_mb', 2)))
        log_backup_count = int(os.getenv('LOG_BACKUP_COUNT', config.get('backup_count', 5)))
        file_log_level = os.getenv('LOG_FILE_LEVEL', config.get('file_level', global_level))
        console_log_level = os.getenv('LOG_CONSOLE_LEVEL', config.get('console_level', global_level))
        use_time_rotation = os.getenv('USE_TIME_ROTATION', str(config.get('use_time_rotation', False)).lower()) in ['true', '1', 'yes']
        log_format = os.getenv('LOG_FORMAT', config.get('log_format', LoggingHandler.DEFAULT_CONFIG['log_format']))

        # Set log folder path
        log_folder = os.getenv('LOG_FOLDER', str(log_folder))

        log_max_size = log_max_size_mb * 1024 * 1024  # Convert MB to bytes

        # Set log file path
        log_file_path = (Path(log_folder) / log_file).resolve()
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, global_level.upper(), logging.DEBUG))

        formatter = logging.Formatter(log_format)

        if use_time_rotation:
            file_handler = LoggingHandler._create_time_rotating_file_handler(
                log_file_path, log_backup_count, file_log_level, formatter
            )
        else:
            file_handler = LoggingHandler._create_file_handler(
                log_file_path, log_max_size, log_backup_count, file_log_level, formatter
            )

        stream_handler = LoggingHandler._create_stream_handler(console_log_level, formatter)

        root_logger.addHandler(file_handler)
        root_logger.addHandler(stream_handler)

        LoggingHandler._suppress_warnings()
        LoggingHandler._redirect_warnings_to_logging(stream_handler)

    @staticmethod
    def _load_config_from_file(config_file: str) -> Dict[str, Any]:
        try:
            with open(config_file, 'r') as f:
                if config_file.endswith('.json'):
                    return json.load(f)
                elif config_file.endswith('.yaml') or config_file.endswith('.yml'):
                    return yaml.safe_load(f)
                else:
                    raise ValueError("Unsupported configuration file format. Use JSON or YAML.")
        except FileNotFoundError:
            config_path = Path(config_file).resolve()
            LoggingHandler._create_default_config_file(config_path)
            return LoggingHandler.DEFAULT_CONFIG

    @staticmethod
    def _create_default_config_file(config_file: Path):
        config_dir = config_file.parent
        config_dir.mkdir(parents=True, exist_ok=True)
        with open(config_file, 'w') as f:
            if config_file.suffix == '.json':
                json.dump(LoggingHandler.DEFAULT_CONFIG, f, indent=4)
            elif config_file.suffix in ['.yaml', '.yml']:
                yaml.dump(LoggingHandler.DEFAULT_CONFIG, f)
            else:
                raise ValueError("Unsupported configuration file format. Use JSON or YAML.")

    @staticmethod
    def _create_file_handler(log_file_path: Path, max_bytes: int, backup_count: int, level: str, formatter: logging.Formatter) -> RotatingFileHandler:
        file_handler = RotatingFileHandler(log_file_path, maxBytes=max_bytes, backupCount=backup_count)
        file_handler.setLevel(getattr(logging, level.upper(), logging.DEBUG))
        file_handler.setFormatter(formatter)
        file_handler.addFilter(LoggingHandler._script_logger_filter)
        return file_handler

    @staticmethod
    def _create_time_rotating_file_handler(log_file_path: Path, backup_count: int, level: str, formatter: logging.Formatter) -> TimedRotatingFileHandler:
        file_handler = TimedRotatingFileHandler(log_file_path, when='midnight', interval=1, backupCount=backup_count)
        file_handler.setLevel(getattr(logging, level.upper(), logging.DEBUG))
        file_handler.setFormatter(formatter)
        file_handler.addFilter(LoggingHandler._script_logger_filter)
        return file_handler

    @staticmethod
    def _create_stream_handler(level: str, formatter: logging.Formatter) -> logging.StreamHandler:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        stream_handler.setFormatter(formatter)
        stream_handler.addFilter(LoggingHandler._script_logger_filter)
        return stream_handler

    @staticmethod
    def _script_logger_filter(record: logging.LogRecord) -> bool:
        current_directory = Path(__file__).parent.resolve()
        return current_directory in Path(record.pathname).resolve().parents

    @staticmethod
    def _suppress_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)

    @staticmethod
    def _redirect_warnings_to_logging(handler: logging.Handler):
        logging.captureWarnings(True)
        warnings_logger = logging.getLogger('py.warnings')
        warnings_logger.addHandler(handler)
        warnings_logger.setLevel(logging.WARNING)

# Example usage:
if __name__ == "__main__":
    # Config can be loaded from a JSON or YAML file
    LoggingHandler.setup_logging()
    # Or using a config dictionary directly
    # config = {
    #     'global_level': 'DEBUG',
    #     'file': 'app.log',
    #     'max_size_mb': 5,
    #     'backup_count': 5,
    #     'file_level': 'DEBUG',
    #     'console_level': 'INFO',
    #     'use_time_rotation': True,
    #     'log_format': '%(asctime)s - %(levelname)s - [%(filename)s: Line %(lineno)d] - %(process)d - %(threadName)s - %(message)s',
    #     'log_folder': 'logs'
    # }
    # LoggingHandler.setup_logging(config=config)
