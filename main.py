from dotenv import load_dotenv
import os
import paho.mqtt.client as mqtt
import app

load_dotenv()

client = client = mqtt.Client()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("Nature_Village/HOUSES/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    app.process_message(msg.topic, msg.payload.decode('utf-8'))

client.on_connect = on_connect
client.on_message = on_message

client.connect(os.getenv('MQTT_URL'), 1883, 60)

client.loop_forever()

