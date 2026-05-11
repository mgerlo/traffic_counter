import json
import paho.mqtt.client as mqtt
import psycopg2
from datetime import datetime, timezone

# Configurazione MQTT
MQTT_HOST = "159.69.51.171"
MQTT_PORT = 1883
MQTT_USER = "terso"
MQTT_PASS = "2OU5GZSB04ocdsjNDTxsK"
MQTT_TOPIC = "/Floud/AutoCounter/48b02dea1de6/count"

# Configurazione Database
DB_HOST = "localhost"
DB_PORT = 5433
DB_NAME = "trafficdb"
DB_USER = "postgres"
DB_PASS = "password"

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def insert_probe_data(conn, data):
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
    print(f"-> Connesso al broker MQTT con codice: {reason_code}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        print(f"___ Ricevuto: {payload}")
        insert_probe_data(userdata['conn'], payload)
    except Exception as e:
        print(f"X Errore: {e}")

def main():
    conn = get_db_connection()
    print("-> Connesso al database")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.user_data_set({'conn': conn})
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    print("_ In ascolto dei messaggi MQTT... (stop per fermare)")

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nX Interrotto dall'utente")
    finally:
        client.disconnect()
        conn.close()
        print("-> Connessioni chiuse correttamente")

if __name__ == "__main__":
    main()
