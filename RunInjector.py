import requests
import time
import json
from requests.auth import HTTPBasicAuth
from datetime import date
from datetime import datetime
from datetime import timedelta
import psycopg2
from pyhive import presto

# Reading from json file, getting the captures and their original IPs
with open('captures_to_inject.json', 'r') as openfile:
    captures_to_inject = json.load(openfile)

# Reading from json file, getting starts IP, and injection cfg parameters
with open('inj_cfg.json', 'r') as openfile:
    inj_cfg = json.load(openfile)

url= inj_cfg["url"]

headers ={
    'Content-type':'application/json',
    'Accept':'*/*'
}

gpdb_user_name=inj_cfg["gpdb_user_name"]
gpdb_pass=inj_cfg["gpdb_pass"]
gpdb_host=inj_cfg["gpdb_host"]
gpdb_port=inj_cfg["gpdb_port"]
gpdb_database=inj_cfg["gpdb_database"]
presto_user_name=inj_cfg["presto_user_name"]
presto_host=inj_cfg["presto_host"]
presto_port=inj_cfg["presto_port"]
presto_catalog=inj_cfg["presto_catalog"]
presto_schema=inj_cfg["presto_schema"]


today = date.today()
current_date = today.strftime("%Y_%m_%d")
day = today.strftime("%d")
file_name = "Stability_results_"+ current_date+ ".txt"
log = open(file_name, "w")

#### biulding IP
ip_third_segment = int(day) + int(inj_cfg["ip_third_segment_start_point"])
start_ip = inj_cfg["ip_first_segment"]+inj_cfg["ip_second_segment"]+str(ip_third_segment) + "."

#### injection section
# need to edit according to amount of captures in file "captures_to_inject.json"
for i in (range(inj_cfg["number_of_loops"])):
    ip = (start_ip + str(int(inj_cfg["ip_fourth_segment_start_point"]+i)))
    # add lines below according to the captures amount
    for j in (range(inj_cfg["number_of_captures"])):
       ((((captures_to_inject['captures'])[j])['manipulationList'])[0])['newValue'] = ip
    json_sends_via_api = json.dumps(captures_to_inject)
    print(json_sends_via_api)
    try:
        r = requests.post(url, json_sends_via_api, headers=headers, auth=HTTPBasicAuth('TimTam', 'TimTam'))
    except:
        print("can not connect to swagger API - Stability test aborted")
        log.write(str(datetime.now()) + ": can not connect to swagger API - Stability test aborted" + "\n")
        exit()
    print(r.content)
    if r.status_code != 200:
        print("Stability test aborted, got status code " + str(r.status_code) + " from the server")
        log.write(str(datetime.now()) + ": Stability test aborted, got status code " + str(r.status_code) + " from the server" + "\n")
        exit()
    time.sleep(inj_cfg["loop_delay"])


log.write(str(datetime.now()) + ": injection finished" + "\n")

time.sleep(1000)

#### verification
#### GPDB
product_types_to_verify = inj_cfg["gpdb_enums"]
try:
    conn= psycopg2.connect(user=gpdb_user_name, password=gpdb_pass, host=gpdb_host, port=gpdb_port, database=gpdb_database)

    for i in range(len(product_types_to_verify)):
        query = "select count(distinct product_id) as amount from merit.dbo.product_participants where ip_address::text like '" + str(start_ip) + "%'and product_start_time > (now () - interval '24 hours') and site_number =1 and product_type=" + str(product_types_to_verify[i]) + ";"
        print(query)
        log.write(str(datetime.now())+ ": "+ query + "\n")
        cursor = conn.cursor()
        cursor.execute(query)
        record = cursor.fetchone()
        if record[0] == inj_cfg["number_of_loops"]:
            result = ": product type " + str(product_types_to_verify[i]) + "- all records arrived to GPDB"
            print(result)
            log.write(str(datetime.now()) + result + "\n")
        elif record[0] > inj_cfg["number_of_loops"]:
            result = ": product type " + str(product_types_to_verify[i]) + " -something wrong in the injection, injected- " + str(inj_cfg["number_of_loops"]) + " got- " + str(record[0])
            print(result)
            log.write(str(datetime.now()) + result + "\n")
        else:
            result = ": product type " + str(product_types_to_verify[i]) + "- got only " + str(record[0]) + " to GPDB"
            print(result)
            log.write(str(datetime.now()) + result + "\n")

except:
    print("can not connect to GPDB")
    log.write(str(datetime.now())+ ": can not connect to GPDB" + "\n")

##### PRESTO

today = date.today()
yesterday = today - timedelta(days=1)
current_date = today.strftime("%Y_%m_%d")
yesterday_date = yesterday.strftime("%Y_%m_%d")

product_types_to_verify = inj_cfg["presto_enums"]
try:
    conn = presto.connect(username=presto_user_name, host=presto_host, port=presto_port, catalog=presto_catalog, schema=presto_schema)

#### find relevant tables

    query = "select table_name from information_schema.tables where table_name like '%xdr%' and table_name not like '%index%' and (table_name  like '%" + str(current_date) + "%' or table_name  like '%" + str(yesterday_date) + "%')"
    cursor = conn.cursor()
    cursor.execute(query)
    tables = cursor.fetchall()


#### data verification on PRESTO
    for i in range(len(product_types_to_verify)):
        amount = 0
        for j in range(len(tables)):
            query = "select count (distinct product_id) as amount from " + str((tables[j])[0]) + " where product_type= " + str(product_types_to_verify[i]) + " and site_id=1 and identifiers like '%"+ start_ip +"%'"
            print(query)
            log.write(str(datetime.now())+ ": "+ query + "\n")
            cursor = conn.cursor()
            cursor.execute(query)
            records = cursor.fetchone()
            amount = amount + records[0]
        if amount == inj_cfg["number_of_loops"]:
            result = " :product type " + str(product_types_to_verify[i]) + "- all records arrived to PRESTO"
            print(result)
            log.write(str(datetime.now())+ result+ "\n")
        elif amount > inj_cfg["number_of_loops"]:
            result= ": product type "  + str(product_types_to_verify[i]) + " -something wrong in the injection, injected- " + str(inj_cfg["number_of_loops"]) + " got- " + str(amount)
            print(result)
            log.write(str(datetime.now())+ result+ "\n")
        else:
            result= ": product type " + str(product_types_to_verify[i]) + "- got only " + str(amount) + " to PRESTO"
            print(result)
            log.write(str(datetime.now())+ result+ "\n")

except:
    print("can not connect to PRESTO")
    log.write(str(datetime.now()) + ": can not connect to PRESTO" + "\n")

log.close()




