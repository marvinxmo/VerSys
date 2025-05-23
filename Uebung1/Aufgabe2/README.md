# Übung1/Aufgabe2

## How to run

Für die Lösung der Aufgabe 2 habe ich ebenfalls Python verwendet.
Die Ausführung ist äquivalent zu der aus Aufgabe 1.

```python
### NUR VENV ERSTELLEN FALLS NOCH NICHT IN AUFGABE 1 GESCHEHEN
### Navigate to Uebung1 dir
cd Uebung1

### Create venv inside Uebung1
python -m venv .venv

### Activate .venv on Windows
.\.venv\Scripts\activate
### or macOS and Linux
source .venv/bin/activate

### Install dependencies from requirement.txt
pip install -r requirements.txt
```

Um den Code auszuführen, tun Sie Folgendes:

```python
### Navigate to Aufgabe1 dir
cd Aufgabe1
### Run the code
python start.py
```

## Dokumentation

Die Klasse Zunder stellt einen Node des Ringnetzwerks dar.

### Grundprinzip

Im dem Ringnetzwerk gibt es einen Host (immer node_id=0). Dieser öffnen einen TCP Port und nimmt JOIN-Requests von anderen Nodes entgegen. Er antwortet mit einem ACK(knowledgement) oder REJECT. Im Falle eines ACK warten der Node auf eine START_SIMULATION Nachricht des Hosts. In dieser enthält er Informationen zur Konfiguration des Ring (initial_p, max_consecutive_misfires) sowie die Informationen aller anderen Teilnehmer (IP und UDP-Port) anschließend wartet er bis er zum erstem mal den Token erhält. Da ich keinen Erflog beim versenden von mulicasts oder broadcasts in meinem Heimnetzwerk hatte, sendet der Node im Falle einer Zündung eine Unicast Nachrichte an alle anderen Teilnehmer.

Außerdem übernimmt das start.py Script nun zentrale Aufgaben. So erfragt es etwa ob eine neuen Ringnetzwerk initialisiert werden soll (Node hosted neue Lobby) oder einem bereits bestehenden Netzwerk beigetreten werden soll. Daher habe ich ein Flowchart erstellt, welches den Prozess darstellt.

![start.py Flowchart](./resources/start_flowchart.png)

Zu Beginn wird nach dem Lobby Port gefragt und überprüft ob auf localhost:port bereits eine Lobby gehostet wird. Wenn ja, kann dieser beigetreten werden oder über die Eingabe einer Host IP einer anderen Lobby mit gleichem Port im Netwerk beigetreten werden. Falls nicht so kann eine neue Lobby auf diesem Port gehostet werden.

Dies ermöglicht es mit einem Rechner mehrere Prozesse in dem Ringnetzwerk zu registrieren, während gleichzeitig Prozesse anderer Rechenern im Netzwerk beitreten können.

Die Zunder-Klasse wurde um die Lobby host und beitritt Funktion erweitert. Die Simulationslogik bleibt aber größtenteils unverändert.

![UML Zunder](./resources/Zunder_UML.png)

Ich habe die folgenden Ergebnisse erzielt:
Mit Parametern: xxx
Ergebnisse: xxx
