import json
import paho.mqtt.client as mqtt
import psycopg2
from datetime import datetime, timezone

# Configurazione MQTT
# Parametri di connessione al broker MQTT remoto della telecamera

MQTT_HOST = "159.69.51.171"     # Indirizzo IP del broker MQTT
MQTT_PORT = 1883                # Porta standard MQTT
MQTT_USER = "terso"             # Username di autenticazione
MQTT_PASS = "2OU5GZSB04ocdsjNDTxsK"  # Password di autenticazione
MQTT_TOPIC = "/Floud/AutoCounter/48b02dea1de6/count"  # Topic da cui ricevere i dati

# Configurazione Database
# Parametri di connessione al database PostgreSQL/TimescaleDB locale

DB_HOST = "localhost"   # Il database gira in locale tramite Docker
DB_PORT = 5433          # Porta mappata dal container Docker (5433 -> 5432)
DB_NAME = "trafficdb"   # Nome del database dedicato al progetto
DB_USER = "postgres"    # Utente PostgreSQL di default
DB_PASS = "password"    # Password impostata alla creazione del container

def get_db_connection():
    """
    Apre e restituisce una connessione al database PostgreSQL/TimescaleDB.
    Viene chiamata una sola volta all'avvio dello script.
    La connessione resta aperta per tutta la durata dell'esecuzione
    e viene chiusa solo alla fine nel blocco finally del main.
    """
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def upsert_probe(conn, data):
    """
    Inserisce o aggiorna l'anagrafica di una probe nella tabella probes.

    Parametri:
        conn  -- la connessione attiva al database
        data  -- il dizionario Python ottenuto dal JSON del messaggio MQTT

    Note:
        - Usa INSERT ... ON CONFLICT DO UPDATE (upsert) per gestire
          sia il primo inserimento che eventuali aggiornamenti futuri
          dei dati anagrafici (nome, coordinate) senza generare errori
        - Viene chiamata ad ogni messaggio ricevuto, ma aggiorna
          il database solo se i dati sono effettivamente cambiati
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO probes (pid, sid, name, lon, lat)
                VALUES (%(pid)s, %(sid)s, %(name)s, %(lon)s, %(lat)s)
                ON CONFLICT (pid) DO UPDATE SET
                    sid  = EXCLUDED.sid,
                    name = EXCLUDED.name,
                    lon  = EXCLUDED.lon,
                    lat  = EXCLUDED.lat
            """, {
                'pid':  data.get('pid'),
                'sid':  data.get('sid'),
                'name': data.get('name'),
                'lon':  float(data.get('lon', 0)),
                'lat':  float(data.get('lat', 0))
            })
            # Conferma la transazione
            conn.commit()

    except Exception as e:
        # Annulla la transazione per mantenere il database in uno stato consistente
        conn.rollback()
        print(f"X Errore aggiornamento anagrafica probe: {e}")
        raise

def insert_probe_data(conn, data):
    """
    Inserisce una singola riga nella tabella probe_data della Hypertable.

    Parametri:
        conn  -- la connessione attiva al database
        data  -- il dizionario Python ottenuto dal JSON del messaggio MQTT

    Note:
        - Il timestamp 'tm' arriva come Unix timestamp (secondi dal 1970)
          e viene convertito in un oggetto datetime con fuso orario UTC
          prima dell'inserimento, come richiesto dal tipo TIMESTAMPTZ
        - Contiene solo i dati di transito del veicolo, senza i campi
          anagrafici (name, lon, lat, sid) ora spostati in probes
        - In caso di errore, conn.rollback() annulla la transazione
          per mantenere il database in uno stato consistente,
          poi rilancia l'eccezione verso on_message
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                        INSERT INTO probe_data
                            (pid, tm, typ, fos, len, cls, spd, flg)
                        VALUES 
                            (%(pid)s, %(tm)s, %(typ)s, %(fos)s, 
                             %(len)s, %(cls)s, %(spd)s, %(flg)s)
                        """, {
                            'pid': data.get('pid'),
                            'tm': datetime.fromtimestamp(data.get('tm', 0), tz=timezone.utc),
                            'typ': data.get('typ'),
                            'fos': data.get('fos'),
                            'len': data.get('len'),
                            'cls': data.get('cls'),
                            'spd': data.get('spd'),
                            'flg': data.get('flg')
                        })
            # Conferma la transazione: i dati vengono salvati definitivamente
            conn.commit()
            print(f"-> Inserito: probe={data.get('name')}, classe={data.get('cls')}, velocità={data.get('spd')} km/h")
    except Exception as e:
        # Annulla la transazione per mantenere il database in uno stato consistente
        conn.rollback()
        print(f"X Errore inserimento DB: {e}")
        # Rilancia l'eccezione verso on_message che la cattura nel suo try/except
        raise

def on_connect(client, userdata, flags, reason_code, properties):
    """
    Callback chiamata automaticamente dalla libreria paho-mqtt
    nel momento in cui la connessione al broker MQTT viene stabilita.

    Parametri:
        client      -- l'istanza del client MQTT
        userdata    -- dati utente passati al client (nel nostro caso la connessione db)
        flags       -- flag di risposta del broker
        reason_code -- codice di risposta ('Success' se la connessione è riuscita)
        properties  -- proprietà MQTT5 (non utilizzate)

    Una volta connessi, ci si iscrive al topic della telecamera
    tramite client.subscribe() per iniziare a ricevere i messaggi.
    """
    print(f"-> Connesso al broker MQTT con codice: {reason_code}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    """
    Callback chiamata automaticamente dalla libreria paho-mqtt
    ogni volta che arriva un nuovo messaggio sul topic sottoscritto.
    Corrisponde al passaggio di un veicolo rilevato dalla telecamera.

    Parametri:
        client   -- l'istanza del client MQTT
        userdata -- dati utente passati al client (nel nostro caso la connessione db)
        msg      -- il messaggio MQTT ricevuto, contiene il payload in bytes

    Il payload viene decodificato da bytes a stringa, poi convertito
    in dizionario Python tramite json.loads().
    Vengono chiamate in sequenza:
        1. upsert_probe()      -> aggiorna l'anagrafica del sensore
        2. insert_probe_data() -> inserisce i dati di transito del veicolo
    Il blocco try/except gestisce eventuali errori senza interrompere lo script.
    """
    try:
        payload = json.loads(msg.payload.decode())
        print(f"___ Ricevuto: {payload}")
        # Prima aggiorna l'anagrafica della probe se necessario
        upsert_probe(conn=userdata['conn'], data=payload)
        # Poi inserisce i dati di transito del veicolo
        insert_probe_data(conn=userdata['conn'], data=payload)
    except Exception as e:
        print(f"X Errore: {e}")

def main():
    """
    Funzione principale che orchestra l'intero flusso:
    1. Apre la connessione al database
    2. Crea e configura il client MQTT
    3. Avvia il loop di ascolto infinito
    4. Gestisce l'interruzione e chiude tutto correttamente

    Il metodo loop_forever() blocca l'esecuzione e gestisce internamente
    la ricezione dei messaggi, chiamando on_connect e on_message
    ogni volta che necessario.
    Il blocco try/except/finally garantisce che le connessioni
    vengano sempre chiuse correttamente, anche in caso di errore.
    """
    # Apertura connessione al database
    conn = get_db_connection()
    print("-> Connesso al database")

    # Creazione del client MQTT con API versione 2
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    # Passa la connessione db come userdata, così on_message può accedervi per inserire i dati
    client.user_data_set({'conn': conn})

    # Imposta le credenziali di autenticazione MQTT
    client.username_pw_set(MQTT_USER, MQTT_PASS)

    # Registra le funzioni callback
    client.on_connect = on_connect
    client.on_message = on_message

    # Connessione al broker MQTT
    # keepalive=60 mantiene la connessione attiva con un ping ogni 60 secondi
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    print("_ In ascolto dei messaggi MQTT... (stop per fermare)")

    try:
        # Avvia il loop infinito di ricezione messaggi
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nX Interrotto dall'utente")
    finally:
        # Chiusura ordinata di entrambe le connessioni
        client.disconnect()
        conn.close()
        print("-> Connessioni chiuse correttamente")

# Punto di ingresso dello script
# Garantisce che main() venga chiamata solo se il file viene eseguito direttamente,
# non importato come modulo
if __name__ == "__main__":
    main()
