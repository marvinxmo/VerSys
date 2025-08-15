import subprocess
import time
import os
import argparse
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# --- Konfiguration ---
BASE_DIR = Path("c:/Users/Admin/Documents/mongo-cluster")
BASE_PORT = 2020
MONGOD_PATH = r"C:/Program Files/MongoDB/Server/8.0/bin/mongod.exe"
MONGOS_PATH = r"C:/Program Files/MongoDB/Server/8.0/bin/mongos.exe"
CONFIG_REPL_SET_NAME = "csrs"


def setup_directories(num_shards: int):
    """Erstellt die notwendigen Datenverzeichnisse für den Cluster."""
    print(f"--- Erstelle Verzeichnisse in {BASE_DIR} ---")
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    # Verzeichnis für Config Server
    config_dir = BASE_DIR / "config"
    config_dir.mkdir(exist_ok=True)
    print(f"Verzeichnis '{config_dir}' für Config Server ist bereit.")

    # Verzeichnisse für Shards
    for i in range(num_shards):
        shard_dir = BASE_DIR / f"shard-{i}"
        shard_dir.mkdir(exist_ok=True)
        print(f"Verzeichnis '{shard_dir}' für Shard {i} ist bereit.")


def start_config_server() -> subprocess.Popen:
    """Startet den Config Server."""
    print("\n--- Starte Config Server ---")
    port = BASE_PORT
    node_dir = BASE_DIR / "config"
    log_path = BASE_DIR / "config.log"

    command = [
        MONGOD_PATH,
        "--configsvr",
        "--port",
        str(port),
        "--dbpath",
        str(node_dir),
        "--logpath",
        str(log_path),
        "--replSet",
        CONFIG_REPL_SET_NAME,
        "--bind_ip",
        "127.0.0.1",
    ]
    print(f"Starte Config Server auf Port {port}...")
    process = subprocess.Popen(command)

    # Warte kurz und initialisiere das Replica Set für den Config Server
    time.sleep(5)
    try:
        client = MongoClient(f"mongodb://localhost:{port}/", directConnection=True)
        client.admin.command(
            "replSetInitiate",
            {
                "_id": CONFIG_REPL_SET_NAME,
                "members": [{"_id": 0, "host": f"localhost:{port}"}],
            },
        )
        print("Config Server Replica Set initialisiert.")
        client.close()
    except Exception as e:
        if "already initialized" in str(e):
            print("Config Server Replica Set ist bereits initialisiert.")
        else:
            print(f"Fehler bei der Initialisierung des Config Servers: {e}")
            # Beenden, wenn der Config Server nicht initialisiert werden kann
            process.terminate()
            raise

    return process


def start_shards(num_shards: int) -> list:
    """Startet die Shard-Server als einzelne Replica Sets."""
    print("\n--- Starte Shard-Server ---")
    processes = []
    for i in range(num_shards):
        port = BASE_PORT + 1 + i
        node_dir = BASE_DIR / f"shard-{i}"
        log_path = BASE_DIR / f"shard-{i}.log"
        repl_set_name = f"rs-shard{i}"

        command = [
            MONGOD_PATH,
            "--shardsvr",
            "--port",
            str(port),
            "--dbpath",
            str(node_dir),
            "--logpath",
            str(log_path),
            "--replSet",
            repl_set_name,
            "--bind_ip",
            "127.0.0.1",
        ]
        print(f"Starte Shard {i} auf Port {port} als Replica Set '{repl_set_name}'...")
        process = subprocess.Popen(command)
        processes.append(process)

        # Warte kurz und initialisiere das Replica Set für den Shard
        time.sleep(2)
        try:
            client = MongoClient(f"mongodb://localhost:{port}/", directConnection=True)
            client.admin.command(
                "replSetInitiate",
                {
                    "_id": repl_set_name,
                    "members": [{"_id": 0, "host": f"localhost:{port}"}],
                },
            )
            print(f"Replica Set '{repl_set_name}' für Shard {i} initialisiert.")
            client.close()
        except Exception as e:
            if "already initialized" in str(e):
                print(f"Replica Set '{repl_set_name}' ist bereits initialisiert.")
            else:
                print(f"Fehler bei der Initialisierung von Shard {i}: {e}")
                process.terminate()
                raise
    return processes


def start_mongos(num_shards: int, num_mongos: int) -> list:
    """Startet die mongos Query Router."""
    print("\n--- Starte mongos Query Router ---")
    processes = []
    config_db_string = f"{CONFIG_REPL_SET_NAME}/localhost:{BASE_PORT}"

    for i in range(num_mongos):
        mongos_port = BASE_PORT + 1 + num_shards + i
        log_path = BASE_DIR / f"mongos-{i}.log"

        command = [
            MONGOS_PATH,
            "--port",
            str(mongos_port),
            "--configdb",
            config_db_string,
            "--logpath",
            str(log_path),
            "--bind_ip",
            "127.0.0.1",
        ]
        print(f"Starte mongos Instanz {i} auf Port {mongos_port}...")
        process = subprocess.Popen(command)
        processes.append(process)

    print(f"\nWarte 10 Sekunden, bis alle Komponenten initialisiert sind...")
    time.sleep(10)
    return processes


def add_shards_to_cluster(num_shards: int, num_mongos: int):
    """Verbindet sich mit mongos und fügt die Shards zum Cluster hinzu."""
    print("\n--- Füge Shards zum Cluster hinzu ---")
    # Verbinde mit der ersten mongos-Instanz, um die Shards hinzuzufügen
    mongos_port = BASE_PORT + 1 + num_shards
    client = None
    for attempt in range(5):
        try:
            client = MongoClient(f"mongodb://localhost:{mongos_port}/")
            # Ping, um Verbindung zu testen
            client.admin.command("ping")
            print("Erfolgreich mit mongos verbunden.")

            for i in range(num_shards):
                shard_port = BASE_PORT + 1 + i
                repl_set_name = f"rs-shard{i}"
                shard_address = f"{repl_set_name}/localhost:{shard_port}"
                try:
                    print(f"Füge Shard {i} ({shard_address}) hinzu...")
                    client.admin.command("addShard", shard_address)
                    print(f"Shard {i} erfolgreich hinzugefügt.")
                except OperationFailure as e:
                    if "already a member" in str(e) or "host already used" in str(e):
                        print(f"Shard {i} ist bereits Teil des Clusters.")
                    else:
                        raise  # Andere Fehler weiterwerfen
            return  # Erfolgreich
        except ConnectionFailure as e:
            print(
                f"Versuch {attempt + 1}: mongos nicht erreichbar, warte 5 Sekunden... ({e})"
            )
            time.sleep(5)
        finally:
            if client:
                client.close()
    print(
        "\n!!! FEHLER: Konnte die Shards nach mehreren Versuchen nicht hinzufügen. !!!"
    )


def main():
    """Hauptfunktion zum Parsen der Argumente und Ausführen der Schritte."""
    parser = argparse.ArgumentParser(
        description="Startet einen lokalen, geshardeten MongoDB Cluster."
    )
    parser.add_argument(
        "--shards",
        type=int,
        default=2,
        help="Die Anzahl der Shards im Cluster (Standard: 2)",
    )
    parser.add_argument(
        "--mongos",
        type=int,
        default=1,
        help="Anzahl der zu startenden mongos Router-Instanzen.",
    )
    args = parser.parse_args()

    if args.shards <= 0:
        print("Fehler: Die Anzahl der Shards muss größer als 0 sein.")
        return
    if args.mongos <= 0:
        print("Fehler: Die Anzahl der mongos-Instanzen muss größer als 0 sein.")
        return

    print(
        f"MongoDB Sharded Cluster mit {args.shards} Shard(s) und {args.mongos} mongos-Instanz(en) wird gestartet."
    )

    all_processes = []
    try:
        setup_directories(args.shards)
        config_process = start_config_server()
        all_processes.append(config_process)

        shard_processes = start_shards(args.shards)
        all_processes.extend(shard_processes)

        mongos_processes = start_mongos(args.shards, args.mongos)
        all_processes.extend(mongos_processes)

        add_shards_to_cluster(args.shards, args.mongos)

        mongos_port = BASE_PORT + 1 + args.shards
        print("\n--- Cluster ist betriebsbereit ---")
        print(f"Verbinde dich z.B. mit: mongodb://localhost:{mongos_port}/")
        if args.mongos > 1:
            print(
                f"Weitere mongos-Instanzen sind bis Port {mongos_port + args.mongos - 1} verfügbar."
            )
        print("\nDrücke Strg+C in diesem Fenster, um alle Cluster-Knoten zu beenden.")

        # Auf Strg+C warten
        while True:
            time.sleep(1)

    except (KeyboardInterrupt, Exception) as e:
        if isinstance(e, Exception) and not isinstance(e, KeyboardInterrupt):
            print(f"\nEin Fehler ist aufgetreten: {e}")
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
