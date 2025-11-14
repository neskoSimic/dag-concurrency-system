import json
import os
import threading
from enum import Enum
from queue import Queue, Empty
from typing import Dict, List, Set
import traceback
import multiprocessing as mp


class NodeState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    READY = "READY"


def run_node_python_action(payload: dict) -> str:
    node_id = payload["id"]
    action  = payload["action"] or {}

    code = action.get("code", "")
    exec(code, {})
    return f"[{node_id}] Python akcija završena"


class Node:
    def __init__(self, node_id, node_capacity, node_deps, node_action,
                 node_resources, node_inputs, node_outputs):
        self.id: str = node_id
        self.deps: list[str] = list(node_deps or [])
        self.action: dict | None = node_action
        self.resources: dict = dict(node_resources or {})
        self.inputs: list[str] = list(node_inputs or [])
        self.outputs: list[str] = list(node_outputs or [])

        self.lock = threading.Lock()
        self.state: str = NodeState.PENDING

        # koliko još zavisnosti nije DONE u podgrafu (inic. na broj deps)
        self.pending_deps_left: int = len(self.deps)

        # opciono: poslednja greška/keš za clean()
        self.last_error: str | None = None
        self.cache: dict = {}

    def reset(self):
        #vraca cvor u pocetno stanje
        with self.lock:
            self.state = NodeState.PENDING
            self.pending_deps_left = len(self.deps)
            self.last_error = None
            self.cache.clear()


class CommandInterface:
    # svaka komanda se izvrsaljava u posebnoj niti

    def __init__(self, *, resources_registry, thread_pool, global_condition: threading.Condition):
        self.nodes: Dict[str, Node] = {}
        self.targets: List[str] = []
        self._lock = threading.Lock()

        # integracije
        self.resources_registry = resources_registry
        self.thread_pool = thread_pool
        self.global_condition = global_condition

        # stanje sistema
        self._build_in_progress = False
        self._active_planners: Set[Planner] = set()
        self._cancel_requested = False


    def _all_output_paths(self):
        with self._lock: #jer mogu komande clean i load se izvrsavati dok dok zovem ovu metodu pa da lockujem ovdje
            nodes=list(self.nodes.values()) #kopija liste cvorova
        for n in nodes:
            for p in (n.outputs or []):
                    yield p #posaljem jednu putanju pa se pauzira funcvkija,generator, salje jedna item po pozivu i pauzira

    #komande
    def get_subgraph(self, target_id: str, visited=None, stack=None):
        #dobijam podgraf sa dfs
        with self._lock:
            if target_id not in self.nodes:
                raise ValueError(f"Nepoznat target: {target_id}")
        #pocetak dfs prazni skupovi
        if visited is None:
            visited = set()
        if stack is None:
            stack = set()

        if target_id in stack:
            # ciklus u zavisnostima
            raise ValueError(f"Detektovan ciklus u zavisnostima (node: {target_id})")

        if target_id in visited:
            return {}

        stack.add(target_id)
        visited.add(target_id)

        with self._lock:
            node = self.nodes[target_id]

        sub = {target_id: node} # novi mali graf kljuc vrijednost
        # rekurzivno preko zavisnosti
        for dep_id in node.deps or []:
            sub.update(self.get_subgraph(dep_id, visited, stack)) # dfs klasican do kraja pa nazad, udpate spoji rezultat u ejdan dict

        stack.remove(target_id)
        return sub
    def load(self, file_path: str, out_q: Queue):
        #ucitavanje grafa i provjera da li neki cvor trazi vise resursa nego sto uopste ima u kapacitetu
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            capacity = data["capacity"]
            self.targets = data.get("targets", [])
            nodes_raw = data["nodes"]

            with self._lock:
                self.nodes.clear() # brisanje starog grafa
                for nd in nodes_raw:
                    node = Node(
                        node_id=nd["id"],
                        node_capacity=nd.get("resources", {}),
                        node_deps=nd.get("deps", []),
                        node_action=nd.get("action"),
                        node_resources=nd.get("resources", {}),
                        node_inputs=nd.get("inputs", []),
                        node_outputs=nd.get("outputs", []),
                    )
                    self.nodes[node.id] = node # stavljam cvorove u nmapu kada sam ih napravio

                # validacija resursa
                total_cpu = capacity.get("CPU", 0)
                total_ram = capacity.get("RAM", 0)
                for n in self.nodes.values():
                    need_cpu = (n.resources or {}).get("CPU", 0)
                    need_ram = (n.resources or {}).get("RAM", 0)
                    if need_cpu > total_cpu or need_ram > total_ram:
                        raise ValueError(
                            f"Node {n.id} traži više resursa nego što je ukupno dostupno (CPU/RAM)."
                        )

            out_q.put(f"Učitano: {len(self.nodes)} čvorova. Ciljevi: {', '.join(self.targets) or '(nema)'}")
        except Exception as e:
            out_q.put(f"Greška pri učitavanju: {e}")
        finally:
            out_q.put(None)

    def build(self, target_id: str, out_q: Queue):
        #provjera da li postoji takav cvor za build?
        with self._lock:
            if target_id not in self.nodes:
                out_q.put(f"Nepoznat target: {target_id}")
                out_q.put(None)
                return

            self._cancel_requested = False
            self._build_in_progress = True
                                # mogao sam rijesiti sa Rlock ali ovako je cistije i manje kriticno i onda isti threadm oze vise puta da uzme lock
        planner = Planner(   # ja u planeru opet pozivam with self lock za pravljenje podgrafa da pristupi self nodes a isti thread vec drzi lock iz builda, posto je obican threading lock zakljuca sam na sebe i ceka zauvijek
            graph=self,
            target_node_id=target_id,
            resources=self.resources_registry,
            thread_pool=self.thread_pool,
            output_queue=out_q,
            global_condition=self.global_condition,
            )
        with self._lock:
            self._active_planners.add(planner)  # ovdje sam rijesio deadlock tako sto sam plannet pravljenje izbacio iz prvog with self lock ali sam dodao self lock za dodavanje tog novog planera je
        out_q.put(f"Pokrećem build {target_id}...")

        def _run_planner(): #helper nit za planera zove svoju nit i pokrece run petlju
            try:
                planner.start()
                planner.plannerThread.join() #ova nit ceka da planerova nit zavrsi ne blokira cli prompt,obavlja sav builld posao, dispatch planiranje
            except Exception:
                out_q.put("[build] planner exception:")
                out_q.put(traceback.format_exc())  # ispisuije kompletan traceback u konzolu
            finally:
                with self._lock:
                    self._active_planners.discard(planner)
                    if not self._active_planners:
                        self._build_in_progress = False
                out_q.put(None)

        threading.Thread(target=_run_planner, daemon=True).start() #pokrece run planner

    def clean(self, out_q: Queue, *, confirmation_provider=None):
        with self._lock:
            if self._build_in_progress:
                out_q.put("Clean nije dozvoljen dok je build u toku.")
                out_q.put(None)
                return

        # potvrda kao u specifikaciji sto se trazi
        out_q.put('Da li ste sigurni? (unesite "DA" ili "NE")')
        if confirmation_provider is None:
            reply = input("> ").strip()
        else:
            reply = confirmation_provider()

        if str(reply).strip().upper() not in {"DA", "YES", "Y"}:
            out_q.put("Clean otkazan.")
            out_q.put(None)
            return

        # brisanje napravljuenih fajlova
        removed = 0
        for p in set(self._all_output_paths()): # set da izbacimo duplikate
            try:
                if os.path.isfile(p):
                    os.remove(p); removed += 1
            except FileNotFoundError:
                pass

        # reset stanja
        with self._lock:
            for n in self.nodes.values():
               n.reset()

        out_q.put(f"Clean završen. Uklonjeno artefakata: {removed}")
        out_q.put(None)

    def stats(self, out_q: Queue):
        # informacije o stanjima cvorova da li rade ili su zavresili ili cekaju..
        # prebroj stanja
        counts = {NodeState.PENDING: 0, NodeState.READY: 0, NodeState.RUNNING: 0,
                  NodeState.DONE: 0, NodeState.FAILED: 0}
        with self._lock:
            for n in self.nodes.values():
                counts[n.state] = counts.get(n.state, 0) + 1
            active_planners = len(self._active_planners)

        # resursi
        try:
            used_cpu, used_ram, total_cpu, total_ram = self.resources_registry.total_resources_comparison()
            res_line = f"CPU: {used_cpu}/{total_cpu} | RAM: {used_ram}/{total_ram}"
        except Exception:
            res_line = "Resursi: nepoznati"

        try:
            active_threads = self.thread_pool.num_active()
        except Exception:
            active_threads = "nepoznati threadovi"

        out_q.put(
            "\n".join([
                f"Nodes: {len(self.nodes)}",
                (f"States: "
                 f"PENDING={counts[NodeState.PENDING]} "
                 f"READY={counts[NodeState.READY]} "
                 f"RUNNING={counts[NodeState.RUNNING]} "
                 f"DONE={counts[NodeState.DONE]} "
                 f"FAILED={counts[NodeState.FAILED]}"),
                res_line,
                f"Active threads: {active_threads}",
                f"Active planners: {active_planners}",
            ])
        )
        out_q.put(None)

    def describe(self, node_id: str, out_q: Queue):
        with self._lock:
            n = self.nodes.get(node_id)
        if not n:
            out_q.put(f"Nepoznat node: {node_id}")
            out_q.put(None)
            return

        with n.lock:
            out_q.put(
                "\n".join([
                    f"Node: {n.id}",
                    f"State: {n.state}  (deps left: {getattr(n, 'pending_deps_left', len(n.deps))})",
                    f"Deps: {', '.join(n.deps) if n.deps else '(none)'}",
                    f"Inputs: {n.inputs or []}",
                    f"Outputs: {n.outputs or []}",
                    f"Resources: {n.resources or {}}",
                    f"Last error: {getattr(n, 'last_error', None)}",
                ])
            )
        out_q.put(None)

    def cancel(self, out_q: Queue):
        # zaustavi novo zakazivanje poslova, zaustavi planere, otkazi poslove koji nus startovani, pusti da se dovrse poslovi
        with self._lock:
            if not self._build_in_progress and not self._active_planners:
                out_q.put("Nijedan build nije u toku.")
                out_q.put(None)
                return
            self._cancel_requested = True
            planners = list(self._active_planners)

        # zaustavi planere
        for p in planners:
            try:
                p.stop() #ide u planer postavlja running false i notifyall radi, cak i da ima slobodnih resursa, plkaner vise ne stavlja nove popslover u pool, probudi se vidi running false i izadje
            except Exception:
                pass

        # prekini preostale poslove u redu
        try:
            self.thread_pool.terminate() #postavi se close true, sve taskovek oji su u redu a nisu pokrenuti ide exepction runntime error.. a pokrenutiu poslovi se dovrsavcaju, ne mogu se ubiti u pythonu
        except Exception:
            pass

        out_q.put("Zakazivanje zaustavljeno (preostali zadaci otkazani; pokrenuti će se dovršiti).")
        out_q.put(None)

    def exit(self, out_q: Queue):
        # provjera da li ima builodvba koji se rade ili planera aktivnih
        with self._lock:
            build_active = self._build_in_progress or bool(self._active_planners)

        if build_active:
            # pozovi cancel ali ne prekidaj poruke
            _empty_q = Queue()
            self.cancel(_empty_q)  # da se ne salju porukje prilikom gasenja kao zakzaivanje zaustavljeno bla bla, nema smisla,da ide kroz glavnom out_q

        # uredno ugasi pool
        try:
            self.thread_pool.close() # ne prima vise zadatke
            self.thread_pool.join() #saceka da se sver aktivne niti dovrse pa onda ih zatvara
        except Exception:
            pass

        out_q.put("Sistem se gasi…")
        out_q.put(None)

class BuildSystem:

    def __init__(self, command_interface):
        self.ci = command_interface
        self._jobs = []         #lista workr niti
        self._printers = []   #lista printer niti
        self._lock = threading.Lock()

    def _start_command(self, target_fn, *args, **kwargs): #ne znam unparijed broj argumeneta jer imamo out_q na kraju za args tuple, rasppacuj sve keyword argumente kwars ako ih ima
        out_q = Queue()

        worker = threading.Thread(target=target_fn, args=(*args, out_q), kwargs=kwargs, daemon=True)
        worker.start()

        #printer nit cita poruke iz queue i stampa dok ne dobije None na kraju, znak za kraj
        def printer():
            while True:
                try:
                    msg = out_q.get(timeout=0.1)
                    if msg is None:
                        break
                    print(msg, flush=True)
                except Empty:
                    #ne prekidaj ako je worker poslao none jer je komanda mozda poklenura planer niti koji ce poslatri jos poruka
                    continue

        p = threading.Thread(target=printer, daemon=True)
        p.start()

        with self._lock: #zabiljezimo zbog ciscenja kao
            self._jobs.append((worker, out_q))
            self._printers.append(p)

    def _cleanup_not_active(self):

        with self._lock:
            alive_jobs = []
            for (w, q) in self._jobs:
                if w.is_alive():
                    alive_jobs.append((w, q))
            self._jobs = alive_jobs

            alive_printers = [t for t in self._printers if t.is_alive()]
            self._printers = alive_printers

    def run(self):
        print(' Komande: load <path>, build <target>, clean, stats, describe <id>, cancel, exit')
        while True:
            try:
                line = input('$ ').strip()
            except (EOFError, KeyboardInterrupt):
                line = 'exit'

            if not line: # ako je prazan unos cekaj dalje
                self._cleanup_not_active()
                continue

            parts = line.split()
            cmd = parts[0].lower()

            try:
                if cmd == 'load' and len(parts) >= 2:
                    self._start_command(self.ci.load, parts[1])

                elif cmd == 'build' and len(parts) >= 2:
                    self._start_command(self.ci.build, parts[1])

                elif cmd == 'clean':
                    self._start_command(self.ci.clean)

                elif cmd == 'stats':
                    self._start_command(self.ci.stats)

                elif cmd == 'describe' and len(parts) >= 2:
                    self._start_command(self.ci.describe, parts[1])

                elif cmd == 'cancel':
                    self._start_command(self.ci.cancel)

                elif cmd == 'exit':
                    # lijepo ugasimo: pokreni exit i
                    self._start_command(self.ci.exit)
                    # dozvoliti printerima da ispišu "Sistem se gasi…"
                    for t in list(self._printers):
                        t.join(timeout=1.0)
                    break

                else:
                    print('Nepoznata komanda ili nedostaje argument.')
            finally:
                self._cleanup_not_active() #osvjezi liste niti na kraju svake komancde

class ResourcesRegistry:
    def __init__(self, total_cpu: int, total_ram: int):
        self._total_cpu = total_cpu
        self._total_ram = total_ram
        self._available_cpu = total_cpu
        self._available_ram = total_ram
        self._lock = threading.Lock()

    def total_resources_comparison(self) -> tuple[int, int, int, int]:
        with self._lock:
            used_cpu = self._total_cpu - self._available_cpu
            used_ram = self._total_ram - self._available_ram
            return (used_cpu, used_ram, self._total_cpu, self._total_ram)

    def try_acquire(self, cpu: int, ram: int) -> bool:
        with self._lock:
            if cpu <= self._available_cpu and ram <= self._available_ram:
                self._available_cpu -= cpu
                self._available_ram -= ram
                return True
            return False

    def release(self, cpu: int, ram: int):
        with self._lock:
            self._available_cpu = min(self._total_cpu, self._available_cpu + cpu)
            self._available_ram = min(self._total_ram, self._available_ram + ram)



class Planner:
    def __init__(self, graph, target_node_id, resources, thread_pool, output_queue: Queue, global_condition: threading.Condition):
        self.graph = graph
        self.target_node_id = target_node_id
        self.resources = resources
        self.global_condition = global_condition
        self.thread_pool = thread_pool
        self.output_queue = output_queue

        # podgraf čvorova za dati ciljni targer cvor
        self.subgraph = set(graph.get_subgraph(target_node_id).keys())
        if not self.subgraph:
            raise ValueError("Nepoznat ciljni podgraf")

        for node_id in self.subgraph:
            node = self.graph.nodes[node_id]
            with node.lock:
                if node.state == NodeState.PENDING:
                    # biće READY tek kad sve deps budu done; pending ostaje ako ima deps jos uvijek
                    all_done = all(self.graph.nodes[d].state == NodeState.DONE for d in node.deps)
                    node.state = NodeState.READY if all_done else NodeState.PENDING #ako su sve njegove zavisotsnti done stavljamo ga u ready stanje, inace je pedning jos uvijek
                # inicijalizuj brojač preostalih zavisnosti ako ga nema
                if not hasattr(node, "pending_deps_left") or node.pending_deps_left is None:
                    node.pending_deps_left = sum(1 for d in node.deps if d in self.subgraph and self.graph.nodes[d].state != NodeState.DONE)

        self.running = True #flag za to da planner radi za run i stop
        self.lock = threading.Lock() # ako planerski thread i glavni thread zajedno pristupe da ne dodje do race condition
        self.plannerThread = None # referenca na nit planera inicijalizacija samo, da nemamo aktivnu nit kasnije dodajemo


    # pokrecemo planersku nit
    def start(self):
        self.plannerThread = threading.Thread(target=self._run, daemon=True)
        self.plannerThread.start()

    def stop(self):
        with self.lock:
            self.running = False
        with self.global_condition:
            self.global_condition.notify_all() #budimo sve koji mozda cekaju na condiiton

    # glavna petlja run
    def _run(self):

        while self.running:
            dispatched = self._dispatch_ready_nodes() #pokusaj da posaljes poslove koji su redi i za kojei ma resursa slobodnih
            #ovo je za drugu verziju ako idemo za komentarisane dijelove dole
            #self._finished_node()

            if self._is_build_completed(): # provjeramo da li je sve gotovo svi cvoroi done prekidamo run
                break
            if not dispatched: # ako nije nista zakanop nema ready stanja cvora ili nema resursa cekaj malo dok se nesto ne pormijeni pozvonice neko
                with self.global_condition:
                    self.global_condition.wait(timeout=0.1)  #kriticno malo zbog wait i notifyall rjsenje da promijenim svagdje prije node.lock wiith selfg global condition refakjtorisanje u planeru i buildiosistemu

        target = self.graph.nodes[self.target_node_id]
        with target.lock:
            msg = "uspio" if target.state == NodeState.DONE else "nije uspio"
        self.output_queue.put(f"[planner] Build {msg}: {self.target_node_id}")
        self.output_queue.put(None) #znak za kraj print niti da je kraj

    def _is_build_completed(self) -> bool:
        # gotovo kada su svi čvorovi u podgrafu done ili failed, kratko
        for node_id in self.subgraph:
            node = self.graph.nodes[node_id]
            with node.lock:
                if node.state not in (NodeState.DONE, NodeState.FAILED):
                    return False
        return True

    #glavno rasporedjivanje
    def _dispatch_ready_nodes(self) -> bool:
        dispatched = False

        for node_id in list(self.subgraph):
            node = self.graph.nodes[node_id]

            with node.lock:
                # ažuriraj ready ako su deps gotove
                if node.state in (NodeState.PENDING, NodeState.READY):
                    pending = sum(1 for d in node.deps if d in self.subgraph and self.graph.nodes[d].state != NodeState.DONE)
                    node.pending_deps_left = pending #koliko ima jos pending cvovora znaci sve zavisnosti moraju biti done
                    if pending == 0 and node.state == NodeState.PENDING:
                        node.state = NodeState.READY

                if node.state != NodeState.READY: #ako nije redi preskacemo
                    continue

                # ako cvor nema ackije, završavamo odmah, bez resursa
                if node.action is None:
                    node.state = NodeState.DONE

                    self._propagate_down(node)
                    dispatched = True
                    continue

                # pokusaj rezervacije resursa
                cpu = (node.resources or {}).get("CPU", 0)
                ram = (node.resources or {}).get("RAM", 0)

                if self.resources.try_acquire(cpu, ram):
                    node.state = NodeState.RUNNING

                    # saljemo u pool
                    is_process = getattr(self.thread_pool, "is_process_pool", lambda: False)()
                    if is_process:
                        load = {"id": node.id, "action": node.action}
                        func = run_node_python_action
                        args = (load  ,)
                    else:
                        func = self._execute_node
                        args = (node,)


                    self.thread_pool.apply_async(  #za drugu verziju ovdje hvatam future i ne koristim callback stavljma na null !!
                        func=func,
                        args=args,
                        callback=self._on_task_done,
                        callback_args=(node, cpu, ram),
                        err_callback=self._on_task_error,
                        err_args=(node, cpu, ram),
                    )
                    """with node.lock: #za drugu verziju
                        node.future = future
                        node.cpu = cpu   
                        node.ram = ram"""

                    dispatched = True


        return dispatched

    #izvrsavanje cvora
    def _execute_node(self, node):

        if node.action is None:
            return f"[{node.id}] node nema akcije"

        code = node.action.get("code", "")
        exec(code)

        return f"[{node.id}] Python akcija završena"

    #callbakovi
    def _on_task_done(self, result, node, cpu, ram): #izvrsava se u worker niti a ne u planerkojs niti
        with node.lock:
            node.state = NodeState.DONE
        self.resources.release(cpu, ram) #pustam resurse iz globalnog registra
        self.output_queue.put(f"[planner] DONE {node.id} → {result}")
        self._propagate_down(node)
        with self.global_condition:
            self.global_condition.notify_all() #zvono da se budi glavna petlja za planersku nit signal

    def _on_task_error(self, exc, node, cpu, ram):# izvrsava se u worker niti ne u planerkojs niti
        with node.lock:
            node.state = NodeState.FAILED
            node.last_error = repr(exc) # da se vidi tacan tip greske kao obj
        self.resources.release(cpu, ram)
        self.output_queue.put(f"[planner] FAILED {node.id}: {exc!r}")
        with self.global_condition:
            self.global_condition.notify_all()


    def _propagate_down(self, finished_node):
        # kada jedan cvor zavrsi mi zelimo da svim njegovim potomcima stavimo za jedna manji brojac preostalih zaviusnosti
        finished_id = finished_node.id
        for nid in self.subgraph:
            n = self.graph.nodes[nid]
            if finished_id in n.deps:
                with n.lock:
                    n.pending_deps_left = max(0, (n.pending_deps_left or 0) - 1)
                    if n.state in (NodeState.PENDING, NodeState.READY) and n.pending_deps_left == 0: #ako ima 0 zavisnsoti postaje ready
                        n.state = NodeState.READY

"""planner i RafThreadPool komuniciraju preko callback funkcija, u skladu sa specifikacijom.
apply_async kreira zadatak sa funkcijom, argumentima, callbackovima i future objektom.
worker nit izvrsava zadatak, popunjava Future, i zatim poziva odgovarajući callback (_on_task_done ili _on_task_error).
callbackovi u Planneru zaduzeni su za mijenjanje stanja cvora, oslobađanje resursa, propagaciju zavisnosti i budjenje planerske petlje.
future je potpuno implementiran (sa result/exception), ali planner koristi callback mehanizam kao primarni način obavjestavanja o zavrsetku zadataka.
"""      """ def _finished_nodes(self): # za drugu verziju
        for node_id in self.subgraph:
            node = self.graph.nodes[node_id]
            with node.lock:
                if getattr(node, "state",None) != NodeState.RUNNING:
                    continue
                fut=getattr(node, "future", None)
                cpu=getattr(node, "cpu", 0)
                ram=getattr(node, "ram", 0)
            if fut is None:
                continue
            if not fut.done():
                continue
            exc=fut.exception()
            if exc is not None:           
                self._on_task_error(exc, node, cpu, ram)
            else:
                result = fut.result()
                self._on_task_done(result, node, cpu, ram)"""

class Future:
    def __init__(self):
        self._exception = None
        self._result = None
        self._done = False # za rezultat ili gresku da je stigla
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def set_result(self, result):
        with self._lock:
            self._result = result
            self._done = True
            self._condition.notify_all()  #javljam osvima koji cekaju rezultat

    def set_exception(self, exc): #sve radimo isto al isada samo za gresku umjesto rezultata
        with self._lock:
            self._exception = exc
            self._done = True
            self._condition.notify_all()

    def result(self, timeout=None):
        with self._lock:
            if not self._done:
                if not self._condition.wait(timeout):  #cekaj da ne bude setreulst ili setexcpetion
                    raise TimeoutError("Zadatak se nije izvršio u roku")
            if self._exception is not None:
                raise self._exception # ponovo bacamo gresku ako je greska
            return self._result

    def done(self): #ide na true ako je zadatak zavrsen ili uspojesno ili greskom
        with self._lock:
            return self._done


    def exepction(self,timeout=None):
        with self._lock:
            if not self._done:
                if not self._condition.wait(timeout):
                    raise RuntimeError("Zadatak se nije izvrsio u roku")
        return self._exception


class RafThreadPool:  #pool je implementiran tako da koristi callback i err_callback za obaveštavanje planera, u skladu sa specifikacijom, dok future ima za opciju sinhrono cekanje rezultata pomocu result i exception metode, nijem i jasno u specifikaciji sta je ispravno????“
    def __init__(self, number_of_threads: int):
        self._number_of_threads = number_of_threads
        self._task_queue = Queue() # red poslova koji se rade
        self._workers = []
        self._closed = False
        self._workers_active = 0
        self._lock = threading.Lock()

        for _ in range(number_of_threads):
            worker = threading.Thread(target=self._worker_loop, daemon=True)
            worker.start()
            self._workers.append(worker)

    def _worker_loop(self):

        while True:
            try:
                task = self._task_queue.get(timeout=1)
            except Empty:
                # izlazaak is petlje ako je pool zatvoren ako jeste izadje i gasi se
                with self._lock:
                    if self._closed:
                        break
                continue

            if task is None:
                # ako je task none zanci da nema sta da radi radnik se gasi
                self._task_queue.task_done()
                break

            func, args, callback, callback_args, err_callback, err_args, future = task # raspakujem zadatak

            with self._lock:
                self._workers_active += 1
            try:
                result = func(*args)
                future.set_result(result)
                if callback is not None:
                    callback(result, *callback_args)
            except Exception as e:
                future.set_exception(e)
                if err_callback is not None:
                    err_callback(e, *err_args)
            finally: # ovaj blok ce se uvijek izvrsitit
                with self._lock:
                    self._workers_active -= 1
                self._task_queue.task_done()

    def apply_async(self, func, args=(), callback=None, callback_args=None,
                    err_callback=None, err_args=None):
        with self._lock:
            if self._closed:
                raise RuntimeError("Pool je zatvoren, ne primaju se novi zadaci") #provjeravamo da li je pool zatvoren

        future = Future()
        callback_args = tuple(callback_args or ())
        err_args = tuple(err_args or ())

        task = (func, args, callback, callback_args, err_callback, err_args, future)
        self._task_queue.put(task)
        return future  #vracam ovaj future da mogu sacekati rezultat za drugu verziju, u prvoj verziji ne kupim ovaj vraceni

    def close(self):
        #  ne prima nove zadatkee  postojeći će se dovršiti
        with self._lock:
            self._closed = True

    def join(self):
        # sačekaj da se svi zadaci obrade
        self._task_queue.join()
        # pošalji kraj svakom radniku a None znaci kraj, oznaka za kraj
        for _ in self._workers:
            self._task_queue.put(None)
        # sačekaj radnike
        for w in self._workers:
            w.join()

    def terminate(self):
        # hitno gašenje: prekid preostalih zadataka u redu
        with self._lock:
            self._closed = True

        while True:
            try:
                task = self._task_queue.get_nowait()
            except Empty:
                break

            if task is None:
                # već poslati kraj – ignoriši
                self._task_queue.task_done()
                continue

            func, args, callback, callback_args, err_callback, err_args, future = task
            exception = RuntimeError("Prekinut je zadatak (terminate)")
            future.set_exception(exception)
            if err_callback is not None:
                err_callback(exception, *err_args)
            self._task_queue.task_done()

        # pošalji kraj svakopm radniku svakom radniku i sacekaj da zavrse
        for _ in self._workers:
            self._task_queue.put(None)
        for w in self._workers:
            w.join()

    def num_active(self) -> int:
        with self._lock:
            return self._workers_active

    def is_closed(self) -> bool:
        with self._lock:
            return self._closed

    def is_process_pool(self) -> bool:
        return False



class RafProcessPool:
    def __init__(self, number_of_threads: int):
        self._processes = number_of_threads
        self._pool = mp.Pool(processes=self._processes)
        self._lock = threading.Lock()
        self._closed = False
        self._active=0

        self._pending_tasks = []

    def apply_async(self, func, args=(), callback=None, callback_args=None,err_callback=None, err_args=None):
        with self._lock:
            if self._closed:
                raise RuntimeError("Pool je zatvoren")


        future = Future()
        callback_args = tuple(callback_args or ()) #da se mogu raspakovati sa *
        err_args = tuple(err_args or ())

        def _task_successful(result):
            try:
                future.set_result(result)
                if callback is not None:
                    callback(result, *callback_args) #sa zvjezdiocm raspakujemo gore callback args
            except Exception as e:
                # Ako callback baci grešku, mapiraj na future.exception kao i kod thread pool-a
                future.set_exception(e)
            finally:
                with self._lock:
                    self._active -= 1

        def _task_err(exc): #kad pukne zadatak
            try:
                future.set_exception(exc)
                if err_callback is not None:
                    err_callback(exc, *err_args)
            finally:
                with self._lock:
                    self._active -= 1

        with self._lock:
            self._active += 1

        #novi posao saljem u pool
        job=self._pool.apply_async(func,args=args,callback=_task_successful,error_callback=_task_err)

        with self._lock:
            self._pending_tasks.append({"job": job, "future": future,
                "cb": callback, "cb_args": callback_args,
                "err_cb": err_callback, "err_args": err_args})


        return future


    def close(self):
        with self._lock:
            self._closed = True
        self._pool.close()

    def join(self):
        self._pool.join() #blokira sve porcese dok ne zavrse

    def terminate(self):
        with self._lock:
            self._closed = True

        with self._lock:
            pending_lista = list(self._pending_tasks)
            self._pending_tasks.clear()

        for task in pending_lista:
            job = task["job"]
            future = task["future"]
            err_callback = task["err_cb"]
            err_args = task["err_args"]

            if not job.ready():
                future.set_exception(RuntimeError("Prekinut je zadatak,terminate procces pool"))
                try:
                    if err_callback is not None:
                        err_callback(RuntimeError("Prekinut je zadatak,terminate procces pool"), *err_args)
                finally:
                    with self._lock:
                        self._active-=1
        self.terminate()

    def num_active(self) -> int:
        with self._lock:
            return self._active

    def is_closed(self) -> bool:
        with self._lock:
            return self._closed

    def is_process_pool(self) -> bool:
        return True
























