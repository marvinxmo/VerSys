import argparse
import threading
import time
import random
from datetime import datetime
from pymongo import MongoClient, WriteConcern
from pymongo.errors import OperationFailure, ConnectionFailure

# Importiere die Cluster-Management-Funktionen
from start_cluster_shards import (
    setup_directories,
    start_config_server,
    start_shards,
    start_mongos,
    add_shards_to_cluster,
    BASE_PORT,
)

# --- Konfiguration ---
DB_NAME = "sharding_test"

# Szenario-spezifische Collection-Namen
COLLECTION_CONTENTION = "inventory"
COLLECTION_DISTRIBUTED = "activity_logs"


def get_mongos_connection(num_shards: int, num_mongos: int) -> MongoClient:
    """Stellt eine Verbindung zu allen mongos-Routern für die Lastverteilung her."""
    # Erstelle eine Liste aller mongos-Hosts
    mongos_hosts = []
    for i in range(num_mongos):
        port = BASE_PORT + 1 + num_shards + i
        mongos_hosts.append(f"localhost:{port}")

    # Verbinde mit allen mongos-Instanzen. PyMongo verteilt die Last automatisch.
    connection_string = f"mongodb://{','.join(mongos_hosts)}/"
    print(f"Verbinde mit mongos-Cluster unter: {connection_string}")
    # Setze w='majority' für garantierte Schreibvorgänge im Replica Set
    return MongoClient(connection_string, w="majority")


def setup_scenario(client: MongoClient, scenario: str):
    """Bereitet die Datenbank für das jeweilige Szenario vor (löscht alte Daten, shardet die Collection)."""
    db = client[DB_NAME]
    if scenario == "contention":
        collection_name = COLLECTION_CONTENTION
        shard_key = {"_id": 1}
        # Lösche alte Collection
        db.drop_collection(collection_name)
        print(f"Collection '{collection_name}' gelöscht.")
        # Erstelle das eine Dokument, um das gekämpft wird
        db[collection_name].insert_one({"_id": "welcome_page", "page_views": 0})
        print("Einzelnes Dokument für 'contention'-Szenario erstellt.")

    elif scenario == "distributed":
        collection_name = COLLECTION_DISTRIBUTED
        shard_key = {"user_id": "hashed"}
        # Lösche alte Collection
        db.drop_collection(collection_name)
        print(f"Collection '{collection_name}' gelöscht.")
    else:
        raise ValueError("Unbekanntes Szenario")
    # Sharding für die Datenbank aktivieren (ignoriert Fehler, falls bereits aktiviert)
    try:
        client.admin.command("enableSharding", DB_NAME)
        print(f"Sharding für Datenbank '{DB_NAME}' aktiviert.")
    except OperationFailure as e:
        if "already enabled" in str(e):
            print(f"Sharding für Datenbank '{DB_NAME}' war bereits aktiviert.")
        else:
            raise

    # Collection sharden
    try:
        client.admin.command(
            "shardCollection", f"{DB_NAME}.{collection_name}", key=shard_key
        )
        print(f"Collection '{collection_name}' geshardet mit Key: {shard_key}")
    except OperationFailure as e:
        if "already sharded" in str(e):
            print(f"Collection '{collection_name}' war bereits geshardet.")
        else:
            raise

    print("\n--- Setup abgeschlossen, starte Benchmark in 3 Sekunden... ---")
    time.sleep(3)


def run_benchmark(
    client: MongoClient, scenario: str, num_threads: int, ops_per_thread: int
):
    """Führt den eigentlichen Benchmark durch und misst die Performance."""

    threads = []

    def contention_worker():
        """Aktualisiert immer dasselbe Dokument."""
        collection = client[DB_NAME][COLLECTION_CONTENTION]
        for _ in range(ops_per_thread):
            collection.update_one({"_id": "welcome_page"}, {"$inc": {"page_views": 1}})

    def distributed_worker():
        """Fügt immer neue, einzigartige Dokumente ein."""
        collection = client[DB_NAME][COLLECTION_DISTRIBUTED]
        for _ in range(ops_per_thread):
            collection.insert_one(
                {
                    "user_id": f"user-{random.randint(1, 1000000)}",
                    "timestamp": datetime.now(),
                    "action": "page_view",
                }
            )

    def distributed_worker_batches():
        """Fügt immer neue, einzigartige Dokumente in Batches ein."""
        collection = client[DB_NAME][COLLECTION_DISTRIBUTED]

        batch_size = 100
        docs_to_insert = []

        for i in range(ops_per_thread):
            docs_to_insert.append(
                {
                    "user_id": f"user-{random.randint(1, 1000000)}",
                    "timestamp": datetime.now(),
                    "action": "page_view",
                }
            )

            # Wenn der Batch voll ist (oder am Ende des Loops), einfügen
            if len(docs_to_insert) >= batch_size or i == ops_per_thread - 1:
                if docs_to_insert:
                    # ordered=False erlaubt MongoDB, die Inserts parallel zu verarbeiten
                    collection.insert_many(docs_to_insert, ordered=False)
                    docs_to_insert = []  # Batch leeren

    worker_func = (
        contention_worker if scenario == "contention" else distributed_worker_batches
    )

    print(
        f"Starte {num_threads} Threads, jeder führt {ops_per_thread} Operationen aus..."
    )

    start_time = time.time()

    for _ in range(num_threads):
        thread = threading.Thread(target=worker_func)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time.time()

    duration = end_time - start_time
    total_ops = num_threads * ops_per_thread
    throughput = total_ops / duration
    avg_latency_ms = (duration / total_ops) * 1000

    print("\n--- Benchmark Ergebnisse ---")
    print(f"Szenario:           {scenario}")
    print(f"Dauer:              {duration:.2f} Sekunden")
    print(f"Gesamtoperationen:  {total_ops}")
    print(f"Durchsatz:          {throughput:.2f} Ops/Sekunde")
    print(f"Client-Latenz (avg): {avg_latency_ms:.4f} ms/Op")


def main():
    parser = argparse.ArgumentParser(description="MongoDB Sharding Performance-Test")
    parser.add_argument(
        "--shards",
        default=2,
        type=int,
        help="Anzahl der Shards für den Cluster.",
    )
    parser.add_argument(
        "--mongos",
        default=1,
        type=int,
        help="Anzahl der mongos Router-Instanzen für den Cluster.",
    )
    parser.add_argument(
        "--scenario",
        choices=["contention", "distributed"],
        required=True,
        help="Das zu testende Workload-Szenario.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=5,
        help="Anzahl der gleichzeitigen Worker-Threads.",
    )
    parser.add_argument(
        "--ops", type=int, default=10000, help="Anzahl der Operationen pro Thread."
    )
    args = parser.parse_args()

    all_processes = []
    client = None
    try:
        # --- 1. Cluster starten ---
        print(
            f"--- Starte MongoDB Cluster mit {args.shards} Shard(s) und {args.mongos} mongos-Instanz(en) ---"
        )
        setup_directories(args.shards)
        config_process = start_config_server()
        all_processes.append(config_process)

        shard_processes = start_shards(args.shards)
        all_processes.extend(shard_processes)

        mongos_processes = start_mongos(args.shards, args.mongos)
        all_processes.extend(mongos_processes)

        add_shards_to_cluster(args.shards, args.mongos)
        print("\n--- Cluster ist betriebsbereit ---")

        # --- 2. Benchmark ausführen ---
        client = get_mongos_connection(args.shards, args.mongos)
        print("\n--- Setup scenario ---")
        setup_scenario(client, args.scenario)
        print("\n--- Starte Benchmark ---")
        run_benchmark(client, args.scenario, args.threads, args.ops)

    except Exception as e:
        print(f"\nEin Fehler ist aufgetreten: {e}")
    finally:
        # --- 3. Aufräumen ---
        if client:
            client.close()
            print("\nVerbindung zu mongos geschlossen.")

        print("\n--- Beende Cluster-Knoten ---")
        # Prozesse in umgekehrter Reihenfolge beenden
        for process in reversed(all_processes):
            if process.poll() is None:  # Nur beenden, wenn er noch läuft
                print(f"Beende Prozess (PID: {process.pid})...")
                process.terminate()

        # Warte kurz, damit die Prozesse Zeit zum Beenden haben
        time.sleep(2)
        print("Alle Knoten wurden beendet.")


if __name__ == "__main__":
    main()
