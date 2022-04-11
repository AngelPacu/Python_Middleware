import time
import worker, master

import threading

if __name__ == "__main__":
    master = threading.Thread(target=master.run_master)
    master.start()
    time.sleep(2.0)
    worker1 = threading.Thread(target=worker.run_worker, args=(1000,))
    worker1.start()
    worker2 = threading.Thread(target=worker.run_worker, args=(2000,))
    worker2.start()
    # worker3 = threading.Thread(target=Worker, args=(3000,))
    # worker3.start()

    # worker = Worker(1000)

