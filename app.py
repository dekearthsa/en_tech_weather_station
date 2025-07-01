import subprocess
import json
import requests
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import pandas as pd
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
client = MongoClient(MONGO_URI)

db = client["rangsit_weather"]          
col = db["weather"]  

USERNAME = ""
PASSWORD = ""
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
        print(transformed)
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
        "rand": "325963483046700",
        "ServerID": "122.155.168.171",
        "NetworkID": "RSSWS11-024",
        "NodeID": "4096",
        "IONumber": "2",
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


def convert_data(data_session, sensor_type):
    records = []
    for item in data_session['data'].findall('Items'):
        io_dt = item.findtext('IODateTime')
        val = item.findtext('Value')
        if io_dt and val:
            records.append({'IODateTime': io_dt, 'Value': float(val), "sensor": sensor_type})
    df = pd.DataFrame(records)
    df_old, result_out = read_db()
    if result_out == False:
        return df
    else:
        ## drop dup df_old ##
        combined = pd.concat([df, df_old], ignore_index=True)
        unique_rows = combined.drop_duplicates(keep=False)
        print(unique_rows.head())
        return unique_rows

def read_token_store():
    try:
        with open("token.json", "r") as f:
            token = json.load(f)
        print(token['Set-Cookie'])
        return token
    except Exception as e:
        print("not found token try login: ",e)
        token = func_login()
        return token
    
def insert_into_db(df):
    df["IODateTime"] = pd.to_datetime(df["IODateTime"]) 
    records = df.to_dict(orient="records")
    if records:                        
        result = col.insert_many(records)
        print("Inserted", len(result.inserted_ids), "documents.")
    else:
        print("DataFrame ว่าง ไม่ได้ insert อะไรเลย")

def read_db():
    day = datetime.today().strftime('%d')
    month = datetime.today().strftime('%m')
    year = datetime.today().strftime('%Y')
    start_date = datetime(int(year), int(month), int(day), 0, 0, 0)
    end_date   = start_date + timedelta(days=1)
    docs = col.find({
    "IODateTime": {
        "$gte": start_date,
        "$lt": end_date
        }
    })
    print("fetch data")
    print(docs)
    print("\n")
    if not docs:
        return [],  False
    else:
        df = pd.DataFrame(docs).drop(columns=["_id"], errors="ignore")
        print(df.head())
        return df,  True

def main():
    token = read_token_store()
    data_session = get_data_humid(token)
    if data_session['session'] == False:
        re_token = read_token_store()
        data_session = get_data_humid(re_token)
        df = convert_data(data_session, "humid")
        insert_into_db(df)
    else:
        df = convert_data(data_session, "humid")
        insert_into_db(df)

    data_session = get_data_temp(token)
    if data_session['session'] == False:
        re_token = read_token_store()
        data_session = get_data_temp(re_token)
        df = convert_data(data_session, "temp")
        insert_into_db(df)
    else:
        df = convert_data(data_session, "temp")
        insert_into_db(df)

main()
