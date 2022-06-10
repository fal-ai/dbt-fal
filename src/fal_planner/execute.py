import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import List

from fal_planner.plan import TaskQueue, load_graph

N_THREADS = 5


def serial_executor(queue: TaskQueue) -> None:
    while task := queue.get_next_task():
        task.execute()
        queue.finish(task)


def thread_based_executor(queue: TaskQueue) -> None:
    with ThreadPoolExecutor(max_workers=N_THREADS) as executor:
        futures = {
            executor.submit(task.execute) for task in queue.iter_available_tasks()
        }
        while futures:
            # Get the first completed futures, mark them done.
            done, futures = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                queue.finish(future.result())

            # And load all the tasks that it was blocking.
            futures |= {
                executor.submit(task.execute) for task in queue.iter_available_tasks()
            }


def main():
    from argparse import ArgumentParser

    from fal_planner.static_graph_2 import graph

    for executor in [serial_executor, thread_based_executor]:
        task_queue = load_graph(graph)
        start_time = time.perf_counter()
        executor(task_queue)
        print(f"{executor.__name__!r} took: {time.perf_counter() - start_time}s")


if __name__ == "__main__":
    main()
