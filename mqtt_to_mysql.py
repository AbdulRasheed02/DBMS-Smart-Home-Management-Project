import paho.mqtt.client as mqtt
import pymysql.cursors
import sys
import json
import time
import calendar
from calendar import timegm
import datetime

# User variable for Gateway ID
myGatewayID = "AE:5F:3E:5F:BF:2B"

# User variable for database name
dbName = "SHM"

# User variables for MQTT Broker connection

# mqttBroker in CSE Lab Server (Local Host)
mqttBroker = "10.0.0.88"
mqttBrokerPort = 1883
mqttUser = "Nitt_Powr2"
mqttPassword = "****"

# mySql database in CSE Lab Server (Local Host)
mysqlHost = "127.0.0.1"
mysqlUser = "root"
mysqlPassword = "password"

# unique identifier for meter (Change the topic of meter from nitt_pow to 1 when reflashing)
meterTopic = 1

# This callback function fires when the MQTT Broker conneciton is established.  At this point a connection to MySQL server will be attempted.
def on_connect(client, userdata, flags, rc):
    print("MQTT Client Connected")

    # Subscription to SENSOR
    #client.subscribe("tele/"+meterTopic+"/SENSOR")
    client.subscribe("tele/nitt_pow/SENSOR")
    try:
        db = pymysql.connect(host=mysqlHost, user=mysqlUser, password=mysqlPassword,
                             db=dbName, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        print("MySQL Client Connected")
        db.close()
    except:
        sys.exit("Connection to MySQL failed")

    # Subscription to STATE
    #client.subscribe("tele/"+meterTopic+"/STATE")
    client.subscribe("tele/nitt_pow/STATE")
    try:
        db = pymysql.connect(host=mysqlHost, user=mysqlUser, password=mysqlPassword,
                             db=dbName, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        print("MySQL Client Connected")
        db.close()
    except:
        sys.exit("Connection to MySQL failed")


# This is the function that updates the SENSOR table with the data from the log.
def log_sensor_telemetry(db, payload):
    cursor = db.cursor()

    meterID=meterTopic
    logTime = payload['Time']
    totalStartTime = time.strptime(
        payload['ENERGY']['TotalStartTime'], "%Y-%m-%dT%H:%M:%S")
    epochTotalStartTime = timegm(totalStartTime)

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

    insertRequest = "INSERT INTO SENSOR(meter_id,Time, TotalStartTime, Total, Yesterday, Today, Period, Power, ApparentPower, ReactivePower, Factor, Voltage, Current) VALUES(%i,%u,%u,%f,%f,%f,%f,%i,%f,%f,%f,%f,%f)" % (
        meterID,logTime, epochTotalStartTime, total, yesterday, today, period, power, apparentPower, reactivePower, factor, voltage, current)

    cursor.execute(insertRequest)
    db.commit()

# Custom function to convert time in "dT:HH:MM:SS" format to totalSeconds
def customTimeFormatToSeconds(time):
    indexOfT = time.index('T')
    day = int(time[:indexOfT])
    hour = int(time[indexOfT+1:indexOfT+3])
    minute = int(time[indexOfT+4:indexOfT+6])
    second = int(time[indexOfT+7:])
    timeinSeconds = int(datetime.timedelta(
        days=day, hours=hour, minutes=minute, seconds=second).total_seconds())
    return timeinSeconds

# This is the function that updates the STATE table from the log.
def log_state_telemetry(db, payload):
    cursor = db.cursor()

    meterID=meterTopic
    time = payload['Time']
    uptime = customTimeFormatToSeconds(payload['Uptime'])
    uptimesec = payload['UptimeSec']
    heap = payload['Heap']
    sleepmode = '"' + payload['SleepMode'] + '"'
    sleep = payload['Sleep']
    loadavg = payload['LoadAvg']
    mqttcount = payload['MqttCount']
    heapused = payload['Berry']['HeapUsed']
    objects = payload['Berry']['Objects']
    power = '"' + payload['POWER'] + '"'
    ap = payload['Wifi']['AP']
    ssid = '"' + payload['Wifi']['SSId'] + '"'
    bssid = '"' + payload['Wifi']['BSSId'] + '"'
    channel = payload['Wifi']['Channel']
    mode = '"' + payload['Wifi']['Mode'] + '"'
    rssi = payload['Wifi']['RSSI']
    signal = payload['Wifi']['Signal']
    linkcount = payload['Wifi']['LinkCount']
    downtime = customTimeFormatToSeconds(payload['Wifi']['Downtime'])

    insertRequest = "INSERT INTO STATE VALUES(%i,%u,%u,%i,%i,%s,%i,%f,%i,%i,%i,%s,%i,%s,%s,%i,%s,%i,%i,%i,%u)" % (
        meterID,time, uptime, uptimesec, heap, sleepmode, sleep, loadavg, mqttcount, heapused, objects, power, ap, ssid, bssid, channel, mode, rssi, signal, linkcount, downtime)

    cursor.execute(insertRequest)
    db.commit()

#Function to add new energy meters into the METER_ID table
def update_devices(db):
    cursor = db.cursor()

    meterID=meterTopic

    # See if meter already exists in METER_ID table, if not insert it.
    deviceQuery = "EXISTS(SELECT * FROM METER_ID WHERE meter_id = %i)"%(meterID)
    cursor.execute("SELECT "+deviceQuery)
    data = cursor.fetchone()

    if(data[deviceQuery] >= 1):
        #Device exists. Update attributes if required
        #updateRequest = 
        #cursor.execute(updateRequest)
        db.commit() 
    else:
        #Device does not exist. Insert into METER_ID table
        insertRequest = "INSERT INTO METER_ID(meter_id) VALUES(%i)" % (meterID)
        cursor.execute(insertRequest)
        db.commit()

# The callback for when a PUBLISH message is received from the MQTT Broker.
def on_message(client, userdata, msg):
    print("Transmission received")
    payload = json.loads((msg.payload).decode("utf-8"))

    #Connecting to mySQL server
    db = pymysql.connect(host=mysqlHost, user=mysqlUser, password=mysqlPassword,
                                 db=dbName, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    #Update METER_ID table 
    update_devices(db)

    # For SENSOR data
    if 'Time' in payload and 'ENERGY' in payload:
        if 'TotalStartTime' in payload['ENERGY'] and 'Total' in payload['ENERGY'] and 'Yesterday' in payload['ENERGY'] and 'Today' in payload['ENERGY'] and 'Period' in payload['ENERGY'] and 'Power' in payload['ENERGY'] and 'ApparentPower' in payload['ENERGY'] and 'ReactivePower' in payload['ENERGY'] and 'Factor' in payload['ENERGY'] and 'Voltage' in payload['ENERGY'] and 'Current' in payload['ENERGY']:
           
            # Function for sensor telemetry table
            log_sensor_telemetry(db, payload)
            print('SENSOR data logged')

    # For STATE data
    if 'Time' in payload and 'Uptime' in payload and 'UptimeSec' in payload and 'Heap' in payload and 'SleepMode' in payload and 'Sleep' in payload and 'LoadAvg' in payload and 'MqttCount' in payload and 'Berry' in payload and 'POWER' in payload and 'Wifi' in payload:
        if 'HeapUsed' in payload['Berry'] and 'Objects' in payload['Berry'] and 'AP' in payload['Wifi'] and 'SSId' in payload['Wifi'] and 'BSSId' in payload['Wifi'] and 'Channel' in payload['Wifi'] and 'Mode' in payload['Wifi'] and 'RSSI' in payload['Wifi'] and 'Signal' in payload['Wifi'] and 'LinkCount' in payload['Wifi'] and 'Downtime' in payload['Wifi']:

            # Function for state telemetry table
            log_state_telemetry(db, payload)
            print('STATE data logged')

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
