import asyncio
import queue
import threading
import time

import gspread


maximum_backoff = 10


class WorkerPool:
    def __init__(self):
        # The queue for tasks
        self.queue = queue.Queue()
        self.threads = []

    # Worker, handles each task
    def worker(self):
        while True:
            with open("log.txt", "a") as f:
                print("Getting queue", file=f)
            item = self.queue.get()
            with open("log.txt", "a") as f:
                print("Received queue", file=f)

            if item is None:
                break
            readd_to_queue = None
            func, finished_flag, worker_sleep = item, FakeFuture(), 0
            while True:
                try:
                    func()
                    finished_flag.set_result('')  # mark as finished
                    break
                except asyncio.exceptions.InvalidStateError as e:
                    with open("log.txt", "a") as f:
                        print(f"Warning: {repr(e)}", file=f)
                except gspread.exceptions.APIError as e:
                    if 'Quota exceeded for quota metric' not in e.response.text:
                        raise e
                    with open("log.txt", "a") as f:
                        print("Quota exceeded... Sleeping...", file=f)
                    time.sleep(worker_sleep)
                    worker_sleep = worker_sleep*2+0.1

            self.queue.task_done()



    def start_workers(self, worker_pool):
        threads = []
        for i in range(worker_pool):
            t = threading.Thread(target=self.worker)
            t.start()
            threads.append(t)
        self.threads = threads


    def stop_workers(self):
        threads = self.threads
        # stop workers
        for i in threads:
            self.queue.put(None)
        for t in threads:
            t.join()


    def add_task(self, task_items):
        self.queue.put(task_items)

    # async def add_task_blocking(self, task_items):
    #     source = asyncio.Future()
    #     self.queue.put((task_items, source, 0))
    #     await source

async def start_async(thing):
    t = asyncio.create_task(thing)
    # what we really need to do here is:
    # Insert t into runnable queue, just before asyncio.current_task(), and switch to it.
    # Only, it is not possible since event loops are just about scheduling callbacks
    await asyncio.sleep(0)
    return t


class FakeFuture:
    def set_result(self, *args):
        return
