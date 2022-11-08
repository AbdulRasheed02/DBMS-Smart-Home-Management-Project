import paho.mqtt.client as mqtt
import pymysql.cursors
import sys
import json
import time
import calendar
from calendar import timegm

# User variable for Gateway ID
myGatewayID = "AE:5F:3E:5F:BF:2B"

# User variable for database name
dbName = "Energy"

# User variables for MQTT Broker connection
mqttBroker = "10.0.0.88"
mqttBrokerPort = 1883
mqttUser = "Nitt_Powr2"
mqttPassword = "****"

# In VPK VM
mysqlHost = "127.0.0.1"
mysqlUser = "root"
mysqlPassword = "password"

# This callback function fires when the MQTT Broker conneciton is established.  At this point a connection to MySQL server will be attempted.


def on_connect(client, userdata, flags, rc):
    print("MQTT Client Connected")

    # Subscribe to STATE also

    client.subscribe("gateway/"+myGatewayID+"/Sensor/#")
    try:
        db = pymysql.connect(host=mysqlHost, user=mysqlUser, password=mysqlPassword,
                             db=dbName, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        db.close()
        print("MySQL Client Connected")
    except:
        sys.exit("Connection to MySQL failed")

# This is the function that updates the SENSOR table with the data from the log.


def log_sensor_telemetry(db, payload):
    cursor = db.cursor()

    time = payload['Time']
    totalStartTime = payload['ENERGY']['TotalStartTime']
    total = payload['ENERGY']['Total']
    yesterday = payload['ENERGY']['Yesterday']
    today = payload['ENERGY']['Today']
    period = payload['ENERGY']['Period']
    power = payload['ENERGY']['Power']
    apparentPower = payload['ENERGY']['ApparentPower']
    reactivePower = payload['ENERGY']['ReactivePower']
    factor = payload['ENERGY']['Factor']
    voltage = payload['ENERGY']['Voltage']
    current = payload['ENERGY']['Current']

    insertRequest = "INSERT INTO SENSOR(Time, TotalStartTime, Total, Yesterday, Today, Period, Power, ApparentPower, ReactivePower, Factor, Voltage, Current) VALUES(%u,%u,%f,%f,%f,%f,%i,%f,%f,%f,%f,%f,%f)" % (
        time, totalStartTime, total, yesterday, today, period, power, apparentPower, reactivePower, factor, voltage, current)
    cursor.execute(insertRequest)
    db.commit()

# The callback for when a PUBLISH message is received from the MQTT Broker.


def on_message(client, userdata, msg):
    print("Transmission received")
    payload = json.loads((msg.payload).decode("utf-8"))

    # For SENSOR subscription
    if 'Time' in payload and 'ENERGY' in payload:
        if 'TotalStartTime' in payload['ENERGY'] and 'Total' in payload['ENERGY'] and 'Yesterday' in payload['ENERGY'] and 'Today' in payload['ENERGY'] and 'Period' in payload['ENERGY'] and 'Power' in payload['ENERGY'] and 'ApparentPower' in payload['ENERGY'] and 'ReactivePower' in payload['ENERGY'] and 'Factor' in payload['ENERGY'] and 'Voltage' in payload['ENERGY'] and 'Current' in payload['ENERGY']:
            db = pymysql.connect(host=mysqlHost, user=mysqlUser, password=mysqlPassword,
                                 db=dbName, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

            # Use this function for device id table
            # sensor_update(db,payload)

            # Function for telemetry table
            log_sensor_telemetry(db, payload)
            print('SENSOR data logged')
            db.close()

    # For STATE subscription
    if 'Time' in payload and 'Uptime' in payload and 'UptimeSec' in payload and 'Heap' in payload and 'SleepMode' in payload and 'Sleep' in payload and 'LoadAvg' in payload and 'MqttCount' in payload and 'Berry' in payload and 'POWER' in payload and 'Wifi' in payload:
        if 'HeapUsed' in payload['Berry'] and 'Objects' in payload['Berry'] and 'AP' in payload['Wifi'] and 'SSId' in payload['Wifi'] and 'BSSId' in payload['Wifi'] and 'Channel' in payload['Wifi'] and 'Mode' in payload['Wifi'] and 'RSSI' in payload['Wifi'] and 'Signal' in payload['Wifi'] and 'LinkCount' in payload['Wifi'] and 'Downtime' in payload['Wifi']:

            # Use this function for device id table
            # sensor_update(db,payload)

            # Function for telemetry table
            log_telemetry(db, payload)
            print('SENSOR data logged')
            db.close()


# Connect the MQTT Client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
# client.username_pw_set(username=mqttUser, password=mqttPassword)
try:
    client.connect(mqttBroker, mqttBrokerPort, 60)
except:
    sys.exit("Connection to MQTT Broker failed")
# Stay connected to the MQTT Broker indefinitely
client.loop_forever()
