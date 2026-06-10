import logging

from yuna.app import main


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Error in main: {e}", exc_info=True)
