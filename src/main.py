import os
import logging
import translation_worker as tw
from logging.handlers import RotatingFileHandler

def setup_logger():
    logs_dir = "/app/logs"
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, "app.log")
    
    handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,
        backupCount=5
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        handlers=[
            handler, 
            logging.StreamHandler()
        ],
        force=True
    )

def main():
    setup_logger()

    # Activates Translation Worker
    translation_worker = tw.TranslationWorker()
    translation_worker.rabbitmq.consume(
        queue_name=translation_worker.request_queue,
        callback=translation_worker.callback
    )

if __name__ == "__main__":
    main()