import json
import sys
import threading
from queue import Queue
from time import sleep

from prvi_projekat import Node, Planner, RafThreadPool, CommandInterface, BuildSystem, ResourcesRegistry,RafProcessPool

if __name__ == "__main__":
    path= "test.json"

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    cap = data.get("capacity", {})
    cpu = int(cap.get("CPU"))
    ram = int(cap.get("RAM"))
    number_thread = cpu

    global_condition = threading.Condition()
    pool = RafThreadPool(number_of_threads=number_thread)
    resources = ResourcesRegistry(total_cpu=cpu, total_ram=ram)
    ci = CommandInterface(
        resources_registry=resources,
        thread_pool=pool,
        global_condition=global_condition
    )
    bs = BuildSystem(ci)
    bs.run()