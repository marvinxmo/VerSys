# Übung3

## How to run

```python
### Navigate to Uebung1 dir
cd Uebung3

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
### Run the code
python start.py
```

Als zu beschriebendes NoSQL-Datenbanksystem habe ich mich für **MongoDB** entschieden.

## Aufgabe1

### MongoDB-Cluster-Architektur

MongoDB verfügt drei verschiedene Tier-Ebenen.

1. Client Tier: Anwendungen kommunizieren in Ihrer Implementierungssprache über den passenden "language driver" mit MongoDB. Diese dient als Verbindung zwischen der Anwendung und der MongoDB-Shell (mongosh)

2. Router Tier: In Falle eines Sharded Cluster dienen "mongos"-Instanzen als Query-Router, der Anfragen an die korrekten Shards weiterleitet. Die "mongos" kommuzieren dazu mit den Config Servern (genau 3 Stück) des Clusters.

3. Data Tier: Die eigentlichen Daten werden in Replica Sets gespeichert, die auf mehreren Servern/Maschinen verteilt sind. Dabei besteht jedes Set aus einem Primary und mehreren Secondary-Nodes. Der Primary wird für Schreibvorgänge angesprochen, die Secondaries replizieren diese Änderungen asynchron. Wird im Rahmen der Hearbeat-Kommunikation ein Ausfall eines Primaries festgestellt, wird automatisch ein Wahlverfahren initiiert, um einen Nachfolger zu bestimmen. Dabei schlagen Secondaries sich selbst als Primary vor und bitten andere Nodes um Stimmen. Eine einfache Mehrheit reicht zum Gewinnen des Quorums aus. Für jede shard wird ein eigenes Replica Set angelegt.

<br>

![MongoDB Architecture](./resources/MongoDB-Arch.webp)

### Konsistenz

Write-Operationen gelten standardmäßig als erfolgreich, wenn mehr als die Hälfte der Nodes (innerhalb eines Replica Sets) den Schreibvorgang bestätigen. Das Quorum-Ziel kann jedoch mithilfe von `writeConcern` beliebig angepasst werden. Hierbei gilt: Ein höheres Ziel verursacht stärkere Kosistenz und Vice versa.
Read-Operationen können auf unterschiedliche Weisen erfolgen. Hier eine Übersicht über die meist verwendeten `readConcern` Konfigurationen:

| `readConcern`    | Beschreibung                                                                                                                                                                                                                                                                                                  |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `"local"`        | (Default) Liest **unbestätigte Daten** vom lokalen Node. Keine Garantien bzgl. Persistenz oder Replikation und dirty reads sind möglich                                                                                                                                                                       |
| `"majority"`     | Liest nur Daten, die von der **Mehrheit der Replica Set-Mitglieder bestätigt** wurden. Bietet Konsistenz auch bei Failovern.                                                                                                                                                                                  |
| `"linearizable"` | Garantiert, dass gelesene Daten **das Ergebnis einer abgeschlossenen Schreiboperation sind**. Anfrage muss direkt an **Primary Node** gesendet werden. Dieser verifiziert seinen Status als Primary und checkt, dass keine neue Wahl in Gange ist. Verursacht hohe Latenz, garantiert aber starke Konsistenz. |

Zudem lässt sich festlegen ob immer von Primary gelesen werden soll oder ob zur Lastverteilung auch von Secondaries gelesen werden kann.

Jedes Replica Set besitzt ein operations log (oplog), eine Liste aller Schreiboperationen die auf dem Primary stattfinden. Jeder Eintrag ist mit einem präzisen logischen Timestamp versehen. Die Secondaries fragen regelmäßig das Oplog des Primary ab und kopieren dieses. Im Falle eines Ausfalls des Primary werden Secondaries deren most recent oplog-Transaktion einen "hohen" Timestamp besitzen, für die Wahl des Nachfolgers bevorzugt.

### PACELC

Mit den default-Settings

1. `writeConcern: { w: 1 }` (bedeutet nur ein Primiray bestätgt write)
2. `readConcern: "local"` (liest Daten aus lokalem Speicher des Primary)

lässt sich MongoDB als PA/EL System klassifizieren. Allerdings lässt sich durch andere Konfigurationen von MongoDB auch die Konsistenz deutlich stärken. So lässt sich durch Anpassen des Quorum-Ziels (`writeConcern: "majority"`) erreichen, dass Schreiboperationen erst dann als erfolgreich gelten, wenn sie von einer Mehrheit der Replica-Set-Mitglieder bestätigt wurden. Ergänzend dazu sorgt ein `readConcern: "majority"` dafür, dass Leseoperationen nur solche Daten zurückgeben, die ebenfalls von einer Mehrheit bestätigt wurden – selbst im Falle eines Failovers bleibt so die Sicht auf bestätigte Daten erhalten.
Für Anwendungen mit besonders hohen Anforderungen an Datenkonsistenz kann zusätzlich `readConcern: "linearizable"` verwendet werden. Damit wird garantiert, dass der Lesevorgang streng sequenziell zum zuletzt erfolgreich bestätigten Schreibvorgang erfolgt – allerdings auf Kosten der Latenz. So kann MongoDB auch als PC/EC-System verwendet werden.
