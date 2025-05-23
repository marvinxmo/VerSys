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

Node 0 ist wieder für das erheben der Statistiken verantwortlich. In einer Erweiterung könnte man nach dem terminieren der Token-Weitergabe auch noch die Ergebnisse durch den Ring propagieren. In der aktuellen Version printed aber nur der ursprüngliche Host die Statistiken ins terminal.

Um die Statistiken zu erheben habe ich ein Ring mit 3 Nodes initialisiert. Davon sind 2 Prozesse auf meinem PC gelaufen und einer auf meinem MacBook.

Um die Ergebnisse zeischen den Experimenten vergleichbar zu halten habe ich mich für folgendes Vorgehen entschieden:

1. Teriminal Ausgaben und sleeps werden auskommentiert
2. Ringnetzwerk mit 3 Nodes (wegen der Hardware-Limitierung in Aufgabe 2)
3. Gemessen wird die erste Runde. Dafür nutze ich eine Zündwahrscheinlichkeit von 1, damit garantiert jeder Node zündet.

Dazu habe ich erneut eine Messung mit meinem Code aus Aufgabe 1 durchgeführt.

|                              |           Anmerkung           | Anzahl Nodes | Zeit der ersten Runde (alle zünden; average über 5 Versuche) |
| :--------------------------- | :---------------------------: | :----------: | :----------------------------------------------------------: |
| Aufgabe 1                    |      alle 3 Nodes auf PC      |      3       |                           2.34 ms                            |
| Aufgabe 2 (PC als Host)      | Node 0: MB<br>Node 1 & 2: PC  |      3       |                           77.33 ms                           |
| Aufgabe 2 (MacBook als Host) | Node 0 & 1: PC<br> Node 2: MB |      3       |                           6.13 ms                            |

Bei Aufgabe 2 ist mir aufgefallen, dass es auch einen signifikanten Unterschied macht wer der Host ist, da dieser die Statistik erfasst und zum Beispiel Rundenzeiten berechnen muss. Hier wird mir mal wieder aufgezeit wie schnell mein MacBook im Vergleich zu meiner Windows Gurke ist. Dafür kann man mein MacBook im Gegenzug ab ca. 5 Chrome Tabs als Laubgebläse zweckentfremden. :D
