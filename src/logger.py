import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(
    name: str = "app_logger",
    level: int = logging.DEBUG,
    log_file: str | None = "app.log",
    log_to_file: bool = True,
) -> logging.Logger:
    """
    Configura e retorna um logger com rotação de arquivos.

    Args:
        name (str): Nome do logger.
        level (int): Nível de logging.
        log_file (Optional[str]): Caminho do arquivo de log.

    Returns:
        logging.Logger: O logger configurado.
    """

    basedir = os.path.abspath(os.path.dirname(__file__))
    log_directory = "logs"
    log_file = os.path.join(basedir, "..", log_directory, log_file)

    # Check if the log directory exists, if not create it
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Create a formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Create a logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if log_to_file:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=5 * 1024 * 1024, backupCount=5, delay=True
        )  # 5 MB per file, 5 backups
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
