import socket
import time

# --- Konfigurierbare Einstellungen ---
BROADCAST_IP = "255.255.255.255"  # Allgemeine Broadcast-Adresse
# Alternativ: Subnetz-spezifische Broadcast-Adresse, z.B. "192.168.1.255"
# Diese musst du ggf. für dein spezifisches Netzwerk anpassen.
BROADCAST_PORT = 10999  # Wähle einen Port
MESSAGE = b"Broadcast Test Message!"
SEND_INTERVAL_SECONDS = 2

# --- Ende Konfiguration ---


def main():
    print(f"Broadcast Sender gestartet.")
    print(f"  Sende an IP:    {BROADCAST_IP}")
    print(f"  Sende an Port:  {BROADCAST_PORT}")
    print(f"  Intervall:      {SEND_INTERVAL_SECONDS} Sekunden")
    print("Sende Nachrichten... (Beenden mit Strg+C)")

    # Erstelle UDP-Socket
    sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # WICHTIG: Erlaube das Senden von Broadcast-Paketen
    try:
        sender_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        print("  SO_BROADCAST auf 1 gesetzt (Broadcast erlaubt).")
    except OSError as e:
        print(
            f"  Fehler beim Setzen von SO_BROADCAST: {e}. Broadcast könnte fehlschlagen."
        )
        # Auf manchen Systemen ist dies nicht zwingend nötig oder schlägt fehl,
        # aber es ist die korrekte Vorgehensweise.

    try:
        message_counter = 0
        while True:
            full_message = MESSAGE + b" #" + str(message_counter).encode()
            print(
                f"Sende: {full_message.decode()} an ({BROADCAST_IP}, {BROADCAST_PORT})"
            )
            try:
                sender_socket.sendto(full_message, (BROADCAST_IP, BROADCAST_PORT))
            except OSError as e:
                print(
                    f"  Fehler beim Senden: {e}. Überprüfe Netzwerk und Broadcast-IP."
                )
                # Mögliche Ursachen: Falsche Broadcast-IP für das Interface, Netzwerkprobleme
            message_counter += 1
            time.sleep(SEND_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nSender wird beendet.")
    finally:
        sender_socket.close()
        print("Sender-Socket geschlossen.")


if __name__ == "__main__":
    main()
