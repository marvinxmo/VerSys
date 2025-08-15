import subprocess
import time
import os
import argparse
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# --- Konfiguration ---
BASE_DIR = Path("c:/Users/Admin/Documents/mongo-cluster")
BASE_PORT = 2020
REPLICA_SET_NAME = "rs0"
MONGOD_PATH = r"C:/Program Files/MongoDB/Server/8.0/bin/mongod.exe"


def setup_directories(num_nodes: int):
    """Erstellt die notwendigen Datenverzeichnisse für jede Instanz."""
    print(f"--- Erstelle Verzeichnisse in {BASE_DIR} ---")
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    for i in range(num_nodes):
        node_dir = BASE_DIR / f"{REPLICA_SET_NAME}-{i+1}"
        node_dir.mkdir(exist_ok=True)
        print(f"Verzeichnis '{node_dir}' ist bereit.")


def start_nodes(num_nodes: int) -> list:
    """Startet die mongod-Prozesse im Hintergrund."""
    print("\n--- Starte MongoDB-Knoten ---")
    processes = []
    for i in range(num_nodes):
        port = BASE_PORT + i
        node_dir = BASE_DIR / f"{REPLICA_SET_NAME}-{i+1}"
        log_path = BASE_DIR / f"{REPLICA_SET_NAME}-{i+1}.log"

        command = [
            MONGOD_PATH,
            "--port",
            str(port),
            "--dbpath",
            str(node_dir),
            "--logpath",
            str(log_path),
            "--replSet",
            REPLICA_SET_NAME,
            "--bind_ip",
            "127.0.0.1",  # Nur lokale Verbindungen erlauben
        ]

        print(f"Starte Knoten {i+1} auf Port {port}...")
        # Startet den Prozess im Hintergrund
        process = subprocess.Popen(command)
        processes.append(process)

    print(
        f"\n{num_nodes} Knoten wurden gestartet. Warte 10 Sekunden, bis sie initialisiert sind..."
    )
    time.sleep(10)  # <-- WICHTIG: Erhöhen Sie diese Zeit
    return processes


def initiate_replica_set(num_nodes: int):
    """Verbindet sich mit dem ersten Knoten und initialisiert das Replica Set."""
    print("\n--- Initialisiere Replica Set ---")
    client = None

    # --- NEUE, ROBUSTERE LOGIK ---
    # Wir versuchen, das Set zu initialisieren. Wenn der Server nicht bereit ist,
    # schlägt es fehl, und wir versuchen es erneut.
    for attempt in range(10):  # Versuche es bis zu 10 Mal
        try:
            # Verbinde dich direkt mit dem ersten Knoten.
            client = MongoClient(
                f"mongodb://localhost:{BASE_PORT}/",
                directConnection=True,
                serverSelectionTimeoutMS=2000,
            )

            # Erstelle die Konfiguration für die Mitglieder
            members = [
                {"_id": i, "host": f"localhost:{BASE_PORT + i}"}
                for i in range(num_nodes)
            ]
            config = {"_id": REPLICA_SET_NAME, "members": members}

            print(f"Versuch {attempt + 1}: Führe rs.initiate() aus...")
            client.admin.command("replSetInitiate", config)

            print("Replica Set erfolgreich initialisiert!")
            return  # Funktion erfolgreich beenden

        except ConnectionFailure as e:
            print(f"Knoten noch nicht erreichbar, warte 2 Sekunden... ({e})")
            time.sleep(2)

        except Exception as e:
            if "already initialized" in str(e):
                print("Replica Set ist bereits initialisiert. Überspringe.")
                return  # Funktion erfolgreich beenden
            else:
                print(f"Initialisierung fehlgeschlagen, versuche erneut... ({e})")
                time.sleep(2)
        finally:
            if client:
                client.close()

    print(
        "\n!!! FEHLER: Konnte das Replica Set nach mehreren Versuchen nicht initialisieren. !!!"
    )


def main():
    """Hauptfunktion zum Parsen der Argumente und Ausführen der Schritte."""
    parser = argparse.ArgumentParser(
        description="Startet einen lokalen MongoDB Cluster."
    )
    parser.add_argument(
        "nodes",
        type=int,
        nargs="?",
        default=3,
        help="Die Anzahl der Knoten im Cluster (Standard: 3)",
    )
    args = parser.parse_args()

    if args.nodes <= 0:
        print("Fehler: Die Anzahl der Knoten muss größer als 0 sein.")
        return

    print(f"MongoDB Cluster mit {args.nodes} Knoten wird gestartet.")

    setup_directories(args.nodes)
    processes = start_nodes(args.nodes)
    initiate_replica_set(args.nodes)

    print("\n--- Cluster ist betriebsbereit ---")
    print(
        f"Verbinde dich mit: mongodb://localhost:{BASE_PORT}/?replicaSet={REPLICA_SET_NAME}"
    )
    print("\nDrücke Strg+C in diesem Fenster, um alle Cluster-Knoten zu beenden.")

    try:
        # Warte auf Benutzer-Input (z.B. Strg+C)
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n--- Beende Cluster-Knoten ---")
        for i, process in enumerate(processes):
            print(f"Beende Prozess für Knoten {i+1} (PID: {process.pid})...")
            process.terminate()  # Sendet das Signal zum Beenden
        print("Alle Knoten wurden beendet.")


if __name__ == "__main__":
    main()
