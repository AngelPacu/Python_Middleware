from worker import Worker
import threading
import logging


def thread_function(name):
    logging.info("Thread %s: starting", name)
    logging.info("Thread %s: finishing", name)


if __name__ == "__main__":
    # main = threading.Thread(target = )

    #worker1 = threading.Thread(target=Worker.__init__, args=(1000,))
    #worker2 = threading.Thread(target=Worker, args=(2000,))
    #worker3 = threading.Thread(target=Worker, args=(3000,))
    worker1 = Worker(1000)

