import subprocess, json, requests, time, threading
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import pandas as pd
from pymongo import MongoClient
import time
import paho.mqtt.client as mqtt


MONGO_URI = "mongodb://root:root@localhost:27017/?authSource=admin"
client = MongoClient(MONGO_URI)

THINGSBOARD_HOST = ""
ACCESS_TOKEN = ""   
PORT = 1883
TOPIC = ""

client_mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client_mqtt.username_pw_set(ACCESS_TOKEN)
mqtt_connected  = threading.Event()
 

db = client["rangsit_weather"]          
col = db["weather"]  

USERNAME = ""
PASSWORD = ""

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅  MQTT connected")
        mqtt_connected.set()
    else:
        print(f"❌  MQTT connect failed rc={rc}")

def on_disconnect(client, userdata, rc):
    mqtt_connected.clear()
    if rc != 0:
        print("⚠️   Unexpected MQTT disconnect – reconnecting …")
        try:
            client.reconnect()
        except Exception as e:
            print("Reconnect error:", e)
    else:
        print("🔌  MQTT disconnected gracefully")

client_mqtt.on_connect    = on_connect
client_mqtt.on_disconnect = on_disconnect
client_mqtt.connect(THINGSBOARD_HOST, PORT, 60)
client_mqtt.loop_start()

 
if not mqtt_connected.wait(10):
    raise RuntimeError("MQTT broker not reachable")

def func_login():
    curl_command = [
        "curl",
        "-X", "POST", "https://ams.rss.co.th/index.php",
        "-H", "Content-Type: application/x-www-form-urlencoded",
        "-d", f"username={USERNAME}&password={PASSWORD}&action=login&login=Sign+in",
        "-s", "-D", "-", "-o", "/dev/null"
    ]

    result = subprocess.run(curl_command, capture_output=True, text=True)

    cookie = None
    for line in result.stdout.splitlines():
        if line.lower().startswith("set-cookie:"):
            raw = line.split("Set-Cookie: ")[1]
            cookie = raw.split(";")[0]  
            break

    if cookie:
        output = { "Set-Cookie": cookie }
        full_cookie = output["Set-Cookie"]
        param, token = full_cookie.split("=", 1)
        transformed = {
            "Set-Cookie": {
                "param": param,
                "token": token
            },
            "full": full_cookie
        }
        # print(transformed)
        with open("token.json", "w") as f:
            json.dump(transformed, f, indent=2)
        return transformed
    else:
        print("No Set-Cookie header found.")

def get_data_humid(token):
    day = datetime.today().strftime('%d')
    month = datetime.today().strftime('%m')
    year = datetime.today().strftime('%Y')
    # print(int(day), int(month), year)
    url = "https://ams.rss.co.th/scripts/php/daily_table.php"
    params = {
        "rand": "372818598586932",
        "ServerID": "122.155.168.171",
        "NetworkID": "RSSWS11-024",
        "NodeID": "4096",
        "IONumber": "2",
        "IOYear": f"{year}",
        "IOMonth": f"{int(month)}",
        "IODayOfMonth": f"{int(day)}",
        # "IOYear": f"2025",
        # "IOMonth": f"6",
        # "IODayOfMonth": f"29",
        "PageNumber": "1",
        "ItemsPerPage": "999"
    }
    cookies = {
        f"{token['Set-Cookie']['param']}": f"{token['Set-Cookie']['token']}"
    }

    response = requests.get(url, params=params, cookies=cookies)
    # print(response.text)
    root = ET.fromstring(response.text)
    session_exp = root.attrib.get("result")
    if session_exp == "Invalid session!":
        return {"session":False, "data":root}
    else: 
        return {"session":True, "data":root}
    
def get_data_temp(token):
    day = datetime.today().strftime('%d')
    month = datetime.today().strftime('%m')
    year = datetime.today().strftime('%Y')
    url = "https://ams.rss.co.th/scripts/php/daily_table.php"
    params = {
        "rand": "354907391054592",
        "ServerID": "122.155.168.171",
        "NetworkID": "RSSWS11-024",
        "NodeID": "4096",
        "IONumber": "1",
        "IOYear": f"{year}",
        "IOMonth": f"{int(month)}",
        "IODayOfMonth": f"{int(day)}",
        "PageNumber": "1",
        "ItemsPerPage": "999"
    }
    cookies = {
        f"{token['Set-Cookie']['param']}": f"{token['Set-Cookie']['token']}"
    }

    response = requests.get(url, params=params, cookies=cookies)
    root = ET.fromstring(response.text)
    session_exp = root.attrib.get("result")
    if session_exp == "Invalid session!":
        return {"session":False, "data":root}
    else: 
        return {"session":True, "data":root}
    
def get_data_light(token):
    day = datetime.today().strftime('%d')
    month = datetime.today().strftime('%m')
    year = datetime.today().strftime('%Y')
    url = "https://ams.rss.co.th/scripts/php/daily_table.php"
    params = {
        "rand": "43575498553518",
        "ServerID": "122.155.168.171",
        "NetworkID": "RSSWS11-024",
        "NodeID": "4096",
        "IONumber": "7",
        "IOYear": f"{year}",
        "IOMonth": f"{int(month)}",
        "IODayOfMonth": f"{int(day)}",
        "PageNumber": "1",
        "ItemsPerPage": "999"
    }
    cookies = {
        f"{token['Set-Cookie']['param']}": f"{token['Set-Cookie']['token']}"
    }

    response = requests.get(url, params=params, cookies=cookies)
    root = ET.fromstring(response.text)
    session_exp = root.attrib.get("result")
    if session_exp == "Invalid session!":
        return {"session":False, "data":root}
    else: 
        return {"session":True, "data":root}


def convert_data(data_session, sensor_type):
    records = []
    for item in data_session['data'].findall('Items'):
        io_dt = item.findtext('IODateTime')
        val = item.findtext('Value')
        if io_dt and val:
            dt_obj = pd.to_datetime(io_dt)
            ts_ms  = int(dt_obj.timestamp() * 1000)
            records.append({'IODateTime': io_dt, 'ms':ts_ms,'Value': float(val), "sensor": sensor_type})
    df = pd.DataFrame(records)
    df_old = read_db(sensor_type)

    if len(df_old) == 0:
        return df
    else:
 
        df["IODateTime"] = pd.to_datetime(df["IODateTime"])
        if df_old["IODateTime"].dtype != "datetime64[ns]":
            df_old["IODateTime"] = pd.to_datetime(df_old["IODateTime"]).dt.tz_localize(None)
        
        df_new = df.merge(
            df_old,
            on=["IODateTime", "Value", "sensor"],
            how="left",
            indicator=True
        )
        df_result = df_new[df_new["_merge"] == "left_only"].drop(columns="_merge")
 
        return df_result

def read_token_store():
    try:
        with open("token.json", "r") as f:
            token = json.load(f)
        return token
    except Exception as e:
        print("not found token try login: ",e)
        token = func_login()
        return token

def re_read_token():
    token = func_login()
    with open("token.json", "w") as f:
        json.dump(token, f, indent=2)
    return token
    
def insert_into_db(df):
    df_sendout  = df.to_dict(orient="records")
    df["IODateTime"] = pd.to_datetime(df["IODateTime"]) 
    records = df.to_dict(orient="records")
    # print(records)
    if records:                        
        result = col.insert_many(records)
        print("Inserted", len(result.inserted_ids), "documents.")
        return df_sendout
    else:
        print("DataFrame not insert no new data.")
        return df_sendout

def read_db(sensor_type):
    day = datetime.today().strftime('%d')
    month = datetime.today().strftime('%m')
    year = datetime.today().strftime('%Y')
    start_date = datetime(int(year), int(month), int(day), 0, 0, 0)
    end_date   = start_date + timedelta(days=1)
    docs = col.find({
    "IODateTime": {
        "$gte": start_date,
        "$lt": end_date
        },
        "sensor": sensor_type
    })
    df = pd.DataFrame(docs).drop(columns=["_id"], errors="ignore")
    return df

def convert_to_thingboard_format(data_1):
    array_payload = []
    for data_2 in data_1:
        for idx_2,i in enumerate(data_2):
            # print(i, idx_2)
            payload = {
                "ts": i['ms'],
                "values":{
                    "temp": float(i['Value']),
                    "humid": float(data_1[1][idx_2]['Value']),
                    "light": float(data_1[2][idx_2]['Value'])
                }
            }
            array_payload.append(payload)
        break
    return array_payload



def send_to_mqtt(records):
    if len(records) == 0:
        return 
    else:
        client_mqtt.publish(TOPIC,json.dumps(records))
        print('sent mqtt')


def main():
    token = read_token_store()
    set_data = []
    data_session = get_data_humid(token)
    # print(data_session)
    
    if data_session['session'] == False:
        re_token = re_read_token()
        data_session = get_data_humid(re_token)
        # print("data_session => ", data_session)
        df = convert_data(data_session, "humid")
        data_json = insert_into_db(df)
        set_data.append(data_json)
    else:
        # print("data_session => ", data_session)
        df = convert_data(data_session, "humid")
        data_json = insert_into_db(df)
        set_data.append(data_json)

    data_session = get_data_temp(token)
    if data_session['session'] == False:
        re_token = re_read_token()
        data_session = get_data_temp(re_token)
        df = convert_data(data_session, "temp")
        data_json = insert_into_db(df)
        set_data.append(data_json)
    else:
        df = convert_data(data_session, "temp")
        data_json = insert_into_db(df)
        set_data.append(data_json)

    data_session = get_data_light(token)
    if data_session['session'] == False:
        re_token = re_read_token()
        data_session = get_data_light(re_token)
        df = convert_data(data_session, "light")
        data_json = insert_into_db(df)
        set_data.append(data_json)
    else:
        df = convert_data(data_session, "light")
        data_json = insert_into_db(df)
        set_data.append(data_json)

    # print(set_data)
    payload = convert_to_thingboard_format(set_data)
    print(payload)
    # send_to_mqtt(payload)



while True:
    main()
    time.sleep(300)   

# main()
