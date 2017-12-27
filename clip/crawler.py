from threading import Thread


class PageCrawler(Thread):
    def __init__(self, name, clip_session, database, work_queue, queue_lock, crawl_function):
        Thread.__init__(self)
        self.name = name
        self.session = clip_session
        self.database_link = database
        self.work_queue = work_queue
        self.queue_lock = queue_lock
        self.crawl_function = crawl_function

    def run(self):
        while True:
            self.queue_lock.acquire()
            if not self.work_queue.empty():
                work_unit = self.work_queue.get()
                self.queue_lock.release()
                self.crawl_function(self.session, self.database_link, work_unit)
            else:
                self.queue_lock.release()
                break
