
worker_list = list()
def register_worker(port):
    worker_list.append(port)


def list_workers():
    return worker_list if worker_list else None


def assign_worker():
    worker_port = worker_list[0] if worker_list else None
    # worker_list.pop(0)
    return worker_port
