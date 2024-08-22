import logging

def configure_logging():
    logging.basicConfig(
        filename='Client.log',
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
