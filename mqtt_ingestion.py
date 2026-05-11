import json
import paho.mqtt.client as mqtt
import psycopg2
from datetime import datetime, timezone

# Configurazione MQTT
# Parametri di connessione al broker MQTT remoto della telecamera

MQTT_HOST = "159.69.51.171"  # Indirizzo IP del broker MQTT
MQTT_PORT = 1883  # Porta standard MQTT
MQTT_USER = "terso"  # Username di autenticazione
MQTT_PASS = "2OU5GZSB04ocdsjNDTxsK"  # Password di autenticazione
MQTT_TOPIC = "/Floud/AutoCounter/48b02dea1de6/count"  # Topic da cui ricevere i dati

# Configurazione Database
# Parametri di connessione al database PostgreSQL/TimescaleDB locale

DB_HOST = "localhost"  # Il database gira in locale tramite Docker
DB_PORT = 5433  # Porta mappata dal container Docker (5433 -> 5432)
DB_NAME = "trafficdb"  # Nome del database dedicato al progetto
DB_USER = "postgres"  # Utente PostgreSQL di default
DB_PASS = "password"  # Password impostata alla creazione del container

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
        - lon e lat arrivano come stringhe nel JSON ('11.226857')
          e vengono convertiti in float per il tipo DOUBLE PRECISION
        - conn.commit() conferma la transazione: senza di esso
          i dati non verrebbero salvati nel database
    """
    with conn.cursor() as cur:
        cur.execute("""
                    INSERT INTO probe_data
                        (pid, name, lon, lat, tm, sid, typ, fos, len, cls, spd, flg)
                    VALUES 
                        (%(pid)s, %(name)s, %(lon)s, %(lat)s, %(tm)s,
                            %(sid)s, %(typ)s, %(fos)s, %(len)s, %(cls)s, %(spd)s, %(flg)s)
                    """, {
                        'pid': data.get('pid'),
                        'name': data.get('name'),
                        'lon': float(data.get('lon', 0)),
                        'lat': float(data.get('lat', 0)),
                        'tm': datetime.fromtimestamp(data.get('tm', 0), tz=timezone.utc),
                        'sid': data.get('sid'),
                        'typ': data.get('typ'),
                        'fos': data.get('fos'),
                        'len': data.get('len'),
                        'cls': data.get('cls'),
                        'spd': data.get('spd'),
                        'flg': data.get('flg')
                    })
        conn.commit()
        print(f"-> Inserito: probe={data.get('name')}, classe={data.get('cls')}, velocità={data.get('spd')} km/h")

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
    Il dizionario viene poi passato a insert_probe_data() per salvarlo nel db.
    Il blocco try/except gestisce eventuali errori senza interrompere lo script.
    """
    try:
        payload = json.loads(msg.payload.decode())
        print(f"___ Ricevuto: {payload}")
        insert_probe_data(userdata['conn'], payload)
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
