import glob
import shutil
from calendar import timegm
from concurrent.futures import ThreadPoolExecutor
import logging
from multiprocessing import Process
import multiprocessing
import sys
import aiohttp
import asyncio
import time
import os
from datetime import datetime
from typing import Literal


class Downloader():

    def __init__(self, quiet, number_process, number_threads_each_process, path, number_hash, mode: Literal["sha1", "ntlm"] = "sha1", file: Literal["single ", "multiple"] = "single"):
        logging.info(f"Downloader start")
        self.number_process = number_process
        self.start_time = time.time()
        self.number_threads_each_process = number_threads_each_process
        os.chdir(path)
        self.number_hash = number_hash
        self.quiet=quiet
        if(self.quiet == False):
            self.pbar = progress_bar(0, self.number_hash)
        self.status = []
        assert mode in ["sha1", "ntlm"], "You can use only sha1 or ntlm"
        self.mode = mode
        assert file in ["single", "multiple"], "You can use only single or multiple"
        self.file = file


    def gen_all__hex(self, i):
        return "{:05X}".format(i)

    def write_to_one_big_file(self):
        files_to_append = glob.glob(".temp*")
        output_file = "individual"

        # apri il file di output in modalità append
        with open(output_file, 'ab') as outfile:
            for file in files_to_append:
                with open(file, 'rb') as infile:
                    shutil.copyfileobj(infile, outfile)
                os.remove(f"{infile.name}")

    async def write_to_file(self, _hex, resp, http_timestamp):
        if (resp.status == 304):
            return
        r = await resp.text()
        if (self.file == "single"):
            filename = f".temp{_hex}"
            with open(f"{filename}", 'w') as f:
                lines = r.splitlines()
                f.writelines(str(_hex) + str(i) + "\n" for i in lines)
        else:
            with open(f"{_hex}", 'w') as f:
                r += "\n"
                f.write(r)
                http_timestamp = http_timestamp + 3600
                os.utime(str(_hex), (http_timestamp, http_timestamp))
                # logging.info(str(_hex) + " creato/modificato")

    async def set_header(self, _hex, sem):
        endpoint = f'https://api.pwnedpasswords.com/range/{_hex}'
        if (self.mode == "ntlm"):
            endpoint += "?mode=ntlm"
        headers = {'User-Agent': "hibp-downloader-python"}
        if (self.file != "single") and (os.path.exists(str(_hex))):
            i_node_modified_timestamp = os.path.getmtime(str(_hex))
            i_node_modified_datatime = datetime.fromtimestamp(
                i_node_modified_timestamp)
            headers['If-Modified-Since'] = i_node_modified_datatime.strftime(
                "%a, %d %b %Y %H:%M:%S GMT")
        await self.get_hash(_hex, sem, headers, endpoint)

    async def get_hash(self, _hex, sem, headers, endpoint):
        async with sem:
            for attempt in range(20):
                time.sleep(attempt)
                try:
                    async with aiohttp.ClientSession(headers=headers) as session:
                        async with session.get(endpoint) as resp:
                            utc_time = time.strptime(resp.headers.get('Last-Modified'), "%a, %d %b %Y %H:%M:%S GMT")
                            http_timestamp = timegm(utc_time)
                            await self.write_to_file(_hex, resp, http_timestamp)
                            if(self.quiet == False):
                                self.pbar.increment()
                            break
                except asyncio.TimeoutError:
                    logging.error(f"{str(_hex)} Timeout error,attemp number:{str(attempt)}, retrying", exc_info=False)
                except aiohttp.ServerDisconnectedError:
                    logging.error(f"{str(_hex)} something else went wrong with the request,attemp number:{str(attempt)}, retrying", exc_info=False)
                except ConnectionResetError:
                    logging.error(f"{str(_hex)} something else went wrong with the request,attemp number:{str(attempt)}, retrying", exc_info=False)
                except aiohttp.ClientConnectionError:
                    logging.error(f"{str(_hex)} the connection was dropped before we finished,attemp number:{str(attempt)}, retrying", exc_info=False)
                except aiohttp.ClientError:
                    logging.error(f"{str(_hex)} something else went wrong with the request,attemp number:{str(attempt)}, retrying", exc_info=False)
                except Exception:
                    logging.error(f"{str(_hex)} something else went wrong,attemp number:{str(attempt)}, retrying", exc_info=False)
            else:
                raise Exception("After 20 attempts, no response was received from the server.")



    async def threads_create_start(self, start_num, end_num):
        tasks = []
        sem = asyncio.Semaphore(self.number_threads_each_process)
        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(
            max_workers=self.number_threads_each_process)
        loop.set_default_executor(executor)
        for number in range(start_num, end_num):
            _hex = self.gen_all__hex(number)
            tasks.append(asyncio.ensure_future(self.set_header(_hex, sem), loop=loop))
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.ALL_COMPLETED
        )
        for task in done:
            exception = task.exception()
            if (exception is not None):
                logging.exception(f"Fatal error in thread {str(task.get_name())} -> Exception: {str(exception)}")
                sys.stderr.write(f"Fatal error in thread {str(task.get_name())} -> Exception: {str(exception)}")
        for task in pending:
            task.cancel()
        executor.shutdown()

    def sub_loop(self, start_num, end_num):
        asyncio.run(self.threads_create_start(start_num, end_num))

    def start(self):
        num_iter = int(self.number_hash / self.number_process)
        list_of_tuple = [(i * num_iter, (i + 1) * num_iter) for i in range(self.number_process)]
        if (self.number_hash % self.number_process != 0):
            extra_tasks = self.number_hash % self.number_process
            tmp_tuple = list_of_tuple.pop()
            list_of_tuple.append((tmp_tuple[0], tmp_tuple[1] + extra_tasks))
        list_of_process = [Process(target=self.sub_loop, args=(i[0], i[1])) for i in list_of_tuple]
        for p in list_of_process:
            p.start()
        for p in list_of_process:
            p.join()
        if (self.file == "single"):
            self.write_to_one_big_file()
        logging.info(f"Downloader finished in {int(time.time() - self.start_time)} seconds.")



class progress_bar(object):
    def __init__(self, initval=0, number_hash=0, quiet=False):
        self.val = multiprocessing.RawValue('i', initval)
        self.lock = multiprocessing.Lock()
        self.number_hash = number_hash
        self.quiet = quiet

    def increment(self):
        with self.lock:
            self.val.value += 1
            print("", end=f"\rScaricati {self.value}/{int(self.number_hash)}\r")


    @property
    def value(self):
        return self.val.value
