import csv
from glob import glob
from itertools import chain
import logging
from multiprocessing import Semaphore
import sys
import time
import os
import re
import click
import psycopg
from typing import Literal
import downloader
import config
import logging.handlers


class Observer():
    def __init__(self, observed, dbname, user, password, host, port):
        self.observed = observed
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def disconnect(self):
        self.database.close()

    def connect(self):
        self.database = psycopg.connect(
            dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port)
        self.database.autocommit = True
        self.cursor = self.database.cursor()

    def dbinsert(self, status):
        try:
            with self.cursor.copy(f"COPY {config.tableNamePostgres} ({config.columnNamePostgres}) FROM STDIN") as copy:
                copy.write(status.read())
        except Exception as e:
            logging.exception("Error during insert, transaction rolled back")
            sys.stderr.write(f"Error during insert, transaction rolled back {str(e)}\n")
            if 'backup.csv' not in status.name:
                os.rename(status.name, status.name.replace('.csv', 'backup.csv'))
            status.close()
            return
        os.remove(status.name)
        status.close()

    def update(self):
        statuses = self.observed.getstatus()
        for status in statuses:
            self.connect()
            self.dbinsert(status)
            self.disconnect()


class MyDownloader(downloader.Downloader):

    def __init__(self, quiet, number_process, number_threads_each_process, path, number_hash, mode: Literal["sha1", "ntlm"] = "sha1", file: Literal["single ", "multiple"] = "single"):
        self.tempAppendSem = Semaphore(1)
        super().__init__(quiet, number_process, number_threads_each_process, path, number_hash, mode, file)

    def getstatus(self):
        return chain(map(open, glob("tempStatus*.csv")))

    def attach(self, observer):
        self.observer = observer

    def notify(self):
        self.observer.update()

    def start(self):
        super().start()
        self.notify()

    async def write_diff_to_temp(self, _hex, resp):
        if (resp.status != 304):
            r = await resp.text()
            r += "\r\n"
            if (os.path.exists(str(_hex))):
                with open(str(_hex), 'r') as f:
                    f = f.read()
                    list_old = re.split(r":\d+\n", f)
                    list_new = re.split(r":\d+\r\n", r)
                    set_to_add = set(list_new).difference(set(list_old))
                    set_to_add.discard('')
                    with self.tempAppendSem:
                        if set_to_add:
                            with open(f'tempStatus{os.getpid()}.csv', 'a+') as f:
                                write = csv.writer(f, delimiter="\n")
                                write.writerow(str(_hex) + str(i) for i in set_to_add)
            else:
                list_new = re.split(r":\d+\r\n", r)
                set_new = set(list_new)
                set_new.discard('')
                with self.tempAppendSem:
                    with open(f'tempStatus{os.getpid()}.csv', 'a+') as f:
                        write = csv.writer(f, delimiter="\n")
                        write.writerow(str(_hex) + str(i) for i in set_new)

    async def write_to_file(self, _hex, resp, http_timestamp):
        await self.write_diff_to_temp(_hex, resp)
        await super().write_to_file(_hex, resp, http_timestamp)
