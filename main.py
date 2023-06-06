import logging
import logging.handlers
import os
import sys
import time
import click
import config
from observerDbPostgres import MyDownloader, Observer


@click.command()
@click.option('--files_path', default='hashes', help='Specify the path where you want to save 16**5 self-updating files.')
@click.option('--quiet',is_flag=True, default=False, help='')
@click.option('--process',  type=int, required=True, help='Specify the number of processes to use.')
@click.option('--thread', type=int, required=True, help='Specify the number of threads to use for each process.')
def main(thread, process, files_path, quiet):
    logging.basicConfig(
        level=logging.INFO,
        handlers=[logging.handlers.SysLogHandler(address = '/dev/log')]
    )
    logging.info(f"Program start")
    start_time = time.time()
    if not os.path.exists(files_path):
        os.makedirs(files_path)
    d = MyDownloader(quiet, process, thread, f'{files_path}', 16**5, "sha1", "multiple")
    d.attach(Observer(d, config.dbNamePostgres, config.usernamePostgres, config.passwordPostgres, config.hostPostgres, config.portPostgres))
    d.start()
    logging.info(f"Program(downloader + DbInsert) finished in {int(time.time() - start_time)} seconds.")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.exception("Fatal error:")
        sys.stderr.write(f"Fatal error: {str(e)}\n")