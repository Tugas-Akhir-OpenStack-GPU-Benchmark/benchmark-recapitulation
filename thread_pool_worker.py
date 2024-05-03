import queue
import threading
import time




class WorkerPool:
    def __init__(self):
        # The queue for tasks
        self.queue = queue.Queue()
        self.threads = []

    # Worker, handles each task
    def worker(self):
        while True:
            item = self.queue.get()
            if item is None:
                break
            item()
            self.queue.task_done()


    def start_workers(self, worker_pool=1000):
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