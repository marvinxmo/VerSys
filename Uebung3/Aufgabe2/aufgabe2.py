import time
import random
import threading
import argparse
from bson.timestamp import Timestamp
from pymongo import MongoClient, ReadPreference, WriteConcern
from pymongo.read_concern import ReadConcern
from pymongo.errors import ConnectionFailure
import json
from threading import Event

# --- Konfiguration ---
CONNECTION_STRING = (
    "mongodb://localhost:2020,localhost:2021,localhost:2022/?replicaSet=rs0"
)
DB_NAME = "voting_app"
COLLECTION_NAME = "votes"

# --- Test-Parameter ---
NUM_WRITE_THREADS = 20  # Erhöhen Sie dies für einen deutlicheren Effekt
WRITES_PER_THREAD = 2000  # Erhöhen Sie dies für einen längeren Lauf

NUM_READ_THREADS = 1
READS_PER_THREAD = 1000


def get_db_connection(mode: str) -> MongoClient:
    """Erstellt eine DB-Verbindung mit der passenden Konfiguration."""
    if mode == "strong_consistency":
        wc = "majority"
        rp = ReadPreference.PRIMARY
        print("Modus: Starke Konsistenz (w='majority', read=PRIMARY)")
    elif mode == "high_throughput":
        wc = 1
        rp = ReadPreference.SECONDARY_PREFERRED
        print("Modus: Hoher Durchsatz (w=1, read=SECONDARY_PREFERRED)")
    else:
        raise ValueError(
            "Unbekannter Modus. Wähle 'strong_consistency' oder 'high_throughput'."
        )
    # KORREKTUR: Das korrekte WriteConcern-Objekt verwenden
    return MongoClient(CONNECTION_STRING, w=wc, read_preference=rp)


def worker_thread_write(client: MongoClient):
    """Ein Thread, der eine definierte Anzahl von Stimmen abgibt."""
    votes_collection = client[DB_NAME][COLLECTION_NAME]

    # Jeder Thread wählt zufällig einen Kandidaten und gibt ihn alle Stimmen
    vote_for = "candidate_A" if random.random() > 0.5 else "candidate_B"

    for _ in range(WRITES_PER_THREAD):
        votes_collection.update_one(
            {"_id": vote_for}, {"$inc": {"count": 1}}, upsert=True
        )


def worker_thread_read(client: MongoClient):
    """Ein Thread, der eine definierte Anzahl von Lesevorgängen durchführt."""
    votes_collection = client[DB_NAME][COLLECTION_NAME]

    for _ in range(READS_PER_THREAD):
        try:
            # Lese die Stimmen für beide Kandidaten
            result = votes_collection.find_one({"_id": "candidate_A"})
            # if result:
            #     print(f"Votes for candidate_A: {result['count']}")
            result = votes_collection.find_one({"_id": "candidate_B"})
            # if result:
            #     print(f"Votes for candidate_B: {result['count']}")
        except ConnectionFailure as e:
            print(f"Verbindungsfehler beim Lesen: {e}")
        time.sleep(0.01)  # Kurze Pause, um die Last zu verteilen


def monitor_consistency_live(stop_event: Event):
    """Zeigt den Replikations-Lag"""
    print("\n--- Live-Konsistenz-Monitor gestartet ---")
    with MongoClient(
        CONNECTION_STRING, read_preference=ReadPreference.PRIMARY
    ) as primary_client:
        primary_coll = primary_client[DB_NAME][COLLECTION_NAME].with_options(
            read_concern=ReadConcern("local")
        )
        # Uncomment the following line if you need to reset the oplog
        # reset_oplog(primary_client)

        lag_info = []

        while not stop_event.is_set():
            try:

                prim_oplog = primary_client.get_database("local").get_collection(
                    "oplog.rs"
                )

                # 1) EIN Snapshot vom RS-Status
                status = primary_client.admin.command("replSetGetStatus")
                members = status.get("members", [])

                # Save members object as JSON file
                # with open("replica_set_status.json", "w") as f:
                #     json.dump(members, f, default=str, indent=2)

                primary_m = next(
                    (m for m in members if m.get("stateStr") == "PRIMARY"), None
                )
                secondaries = [m for m in members if m.get("stateStr") == "SECONDARY"]

                if not primary_m or not secondaries:
                    print("\rWarte auf PRIMARY/SECONDARIES ...", end="")
                    time.sleep(0.5)
                    continue

                # 2) Primary-Zeitstempel
                prim_t = primary_m.get("optimeWritten").get("ts")
                if not prim_t:
                    print("\rKein gültiger Primary-Timestamp ...", end="")
                    time.sleep(0.5)
                    continue

                lag_entry = []

                # 3) Lag pro Secondary berechnen
                for m in secondaries:

                    # Get Time Lag
                    sec_t = m.get("optimeWritten").get("ts")

                    # Calc total ops lag
                    try:

                        n_ops = prim_oplog.count_documents(
                            {
                                "ts": {
                                    "$lte": prim_t,
                                    "$gt": sec_t,
                                },
                                "op": {"$in": ["u"]},  # update operations only
                            }
                        )

                    except Exception as e:
                        n_ops = "?"

                    # Add to lag_entry with JSON-like format
                    lag_entry.append(
                        {
                            "name": m.get("name"),
                            "ReplicationLag": n_ops if isinstance(n_ops, int) else None,
                        }
                    )

                lag_info.append(lag_entry)

                # 4) Terminal output with ReplicationLag information
                sec_display = []
                for entry in lag_entry:
                    name = entry["name"]
                    ops_val = (
                        f"{entry['ReplicationLag']}"
                        if entry["ReplicationLag"] is not None
                        else "?"
                    )
                    sec_display.append(f"{name}: ReplicationLag={ops_val}")

                print(f"[LIVE] Replication Status | {' | '.join(sec_display)}")

                time.sleep(0.5)

            except Exception as e:
                print(f"\nWarnung: {type(e).__name__}: {e}")
                time.sleep(0.5)

    print("\n--- Live-Konsistenz-Monitor beendet ---")


def main():
    parser = argparse.ArgumentParser(description="MongoDB Performance-Test")
    parser.add_argument("mode", choices=["strong_consistency", "high_throughput"])
    args = parser.parse_args()

    client = get_db_connection(args.mode)
    db = client[DB_NAME]
    db[COLLECTION_NAME].drop()
    print(
        f"\nStarte Test mit {NUM_WRITE_THREADS} Threads, je {WRITES_PER_THREAD} Schreibvorgänge..."
    )

    # --- NEU: Setup für den Monitoring-Thread ---
    stop_monitoring_event = Event()
    monitor_thread = None
    # if args.mode == "high_throughput":
    monitor_thread = threading.Thread(
        target=monitor_consistency_live, args=(stop_monitoring_event,)
    )
    monitor_thread.start()

    admin_db = client.admin
    op_counters_before = admin_db.command("serverStatus")["opcounters"]
    start_time = time.time()

    threads = []
    for _ in range(NUM_WRITE_THREADS):
        thread = threading.Thread(target=worker_thread_write, args=(client,))
        threads.append(thread)
        thread.start()

    for _ in range(NUM_READ_THREADS):
        thread = threading.Thread(target=worker_thread_read, args=(client,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # --- NEU: Den Monitoring-Thread sauber beenden ---
    if monitor_thread:
        time.sleep(2)
        stop_monitoring_event.set()
        monitor_thread.join()

    end_time = time.time()

    # ... der Rest des Skripts für die Performance-Messung bleibt gleich ...
    server_status = admin_db.command("serverStatus")
    op_counters_after = server_status["opcounters"]
    op_latencies = server_status.get("opLatencies", {})
    latencies_w = op_latencies.get("writes", {"latency": 0, "ops": 0})
    latencies_r = op_latencies.get("reads", {"latency": 0, "ops": 0})
    duration = end_time - start_time
    total_writes = op_counters_after["update"] - op_counters_before["update"]
    throughput = total_writes / duration
    avg_latency_write_ms = (
        latencies_w["latency"] / latencies_w["ops"] / 1000
        if latencies_w["ops"] > 0
        else 0
    )
    avg_latency_read_ms = (
        latencies_r["latency"] / latencies_r["ops"] / 1000
        if latencies_r["ops"] > 0
        else 0
    )

    print("\n--- Performance-Ergebnisse ---")
    print(f"Dauer: {duration:.2f} Sekunden")
    print(f"Durchsatz: {throughput:.2f} Schreibvorgänge/Sekunde")
    print(f"Durchschnittliche Server-Latenz (writes): {avg_latency_write_ms:.4f} ms")
    print(f"Gesamtzahl der Schreibvorgänge: {total_writes}")
    print(f"Durchschnittliche Server-Latenz (read): {avg_latency_read_ms:.4f} ms")

    client.close()


if __name__ == "__main__":
    main()
