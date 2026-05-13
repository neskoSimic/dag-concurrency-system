A concurrent, DAG-based build system written in Python, inspired by tools like make and ninja.

## What it does

Loads a task graph from a JSON file and executes nodes in dependency order, in parallel,
across multiple threads. Multiple builds can run simultaneously — they share the same graph
and resource registry, with all shared state properly synchronized.

## Features

- **DAG execution** — tasks run only after their dependencies complete
- **Resource-aware scheduling** — each node declares CPU and RAM requirements; the scheduler
  respects global capacity limits
- **Custom thread pool** (`RafThreadPool`) — built from scratch, with `Future` objects,
  callbacks, and proper lifecycle management (`close`, `join`, `terminate`)
- **Multiple concurrent builds** — each `build` command spawns a dedicated Planner thread;
  planners cooperate over the shared graph without copying nodes
- **No busy-waiting** — all synchronization uses condition variables, events, or queues
- **Interactive CLI** — commands: `load`, `build`, `clean`, `stats`, `describe`, `cancel`, `exit`

## JSON input format

```json
{
  "capacity": { "CPU": 4, "RAM": 32 },
  "targets": ["all"],
  "nodes": [
    {
      "id": "compile_a",
      "deps": ["fetch_headers"],
      "action": { "type": "shell", "cmd": "gcc -c src/a.c -o build/a.o" },
      "outputs": ["build/a.o"],
      "resources": { "CPU": 2, "RAM": 1 }
    }
  ]
}
```

## Running

```bash
python main.py
```

Then type commands interactively:
load graph.json
build all
stats
exit

## Node states

`PENDING → READY → RUNNING → DONE / FAILED`

A node becomes `READY` once all its dependencies finish. The planner dispatches it
to the thread pool as soon as enough resources are free.

## Bonus

Includes an optional `MyProcessPool` wrapper around Python's `multiprocessing.Pool`,
drop-in compatible with `RafThreadPool`.
