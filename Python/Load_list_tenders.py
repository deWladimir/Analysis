from datetime import datetime
import dateutil.parser as parser 
import psycopg2
import requests
import json

# define functions
def sendTelegramMessage(message):
    token = "enter_your_token"
    chat_id = 'enter_your_chat'
    apiURL = 'https://api.telegram.org/bot{}/sendMessage'.format(token)

    requests.post(apiURL, json= {"chat_id": chat_id, "text": "LIST_TENDERS\n" + message})
    

def to_iso_format(date):
    return parser.parse(str(date)).isoformat()

def add_0_to_num(num):
    return "0" + str(int(num)) if num <= 9 else str(int(num))

def count_total_time(start_time, end_time):
    time_difference = (end_time - start_time).total_seconds()
    minutes, seconds = divmod(time_difference, 60)
    hours, minutes = divmod(minutes, 60)
    
    time = []
    for val in [seconds, minutes, hours]:
        time.append(add_0_to_num(val))
    
    return "{}:{}:{}".format(time[2], time[1], time[0])

def insert_list_tenders(cursor, data):
    insert_query = """
                    insert into public.list_tenders
                    (
                        date_modified,
                        tender_id,
                        is_load_data,
                        queue_number
                    )
                    values 
                        (%s, %s, false, 1)
                   """
    try:
        cursor.executemany(insert_query, data)
    except Exception as e:
        sendTelegramMessage("Ooopppsss, error while inserting data: {}".format(str(e)))

def select_offset_datetime(cursor):
    data = []

    query_list_tenders = """select 
                                date_modified
                            from public.list_tenders
                            order by date_modified desc
                            limit 1"""
    cursor.execute(query_list_tenders)
    data = cursor.fetchall()

    if data:
        list_tenders_date_modified = to_iso_format(date = data[0][0])
        return list_tenders_date_modified
    else:
        query_tenders = """select 
                                date_modified
                            from public.tenders
                            order by date_modified desc
                            limit 1"""
        cursor.execute(query_tenders)
        data = cursor.fetchall()
        if data:
            tenders_date_modified = to_iso_format(date = data[0][0])
            return tenders_date_modified
        else:
            return parser.parse('2020-01-01').isoformat()
        
# main 

start_time = datetime.now()  
sendTelegramMessage("Starting our noble duty at {}".format(start_time.strftime("%d.%m.%Y %H:%M:%S")))

# connect to database
conn_str = "host = 'localhost' dbname = 'prozzoro_load_data' user = 'postgres' password = 'password' port = 5432"
is_connected_to_db = False
try:
    conn = psycopg2.connect(conn_str)
    conn.autocommit = True
    cursor = conn.cursor()
    is_connected_to_db = True
except Exception as e:
    sendTelegramMessage("Oopppsss, some problems with connection to database: {}".format(str(e)))

# get last dateModified
datetime_offset = select_offset_datetime(cursor = cursor)

# getting data from api and inserting into database
if datetime_offset and is_connected_to_db:
    insert_data = []
    inserted_data_qnt = 0
    url = "https://public.api.openprocurement.org/api/2.5/tenders?offset={}"

    while True:
        try:
            #print("Trying to extract data for {}".format(parser.parse(str(datetime.fromtimestamp(datetime_offset) if not isinstance(datetime_offset, str) else datetime_offset)).isoformat()))

            # making request
            request = requests.get(url.format(datetime_offset))
            all_data = json.loads(request.text)
            # getting tender data and next_page_offset
            tender_data = all_data["data"]
            datetime_offset = all_data["next_page"]["offset"]

            # if tender data is not empty
            if tender_data:
                for tender in tender_data:
                    tender = {
                        "dateModified": str(datetime.fromisoformat(tender['dateModified'])),
                        "id": tender['id']
                    }

                    insert_data.append(tuple(tender.values()))

                # not to collect much data in one list, make insert when it's more than 5000 tuples 
                if len(insert_data) >= 5000:
                    qnt = len(insert_data)

                    insert_list_tenders(cursor = cursor, data = insert_data)
                    sendTelegramMessage("Inserted {} tenders with the last modified date of {}".format(qnt, datetime.fromisoformat(insert_data[qnt - 1][0])))

                    insert_data = []
                    inserted_data_qnt += qnt
            # when got no data from api, stop 
            else:
                break
        except Exception as e:
            sendTelegramMessage("Some problem occured while making request or parsing data: {}\n".format(str(e)) + "Error occured on this datetime offset: {} or previous one".format(datetime_offset))
            break

    # if there are some data left (it's less than 5000 tuples), insert it
    if insert_data:
        qnt = len(insert_data)
        insert_list_tenders(cursor = cursor, data = insert_data)
        sendTelegramMessage("Inserted {} tenders with the last modified date of {}".format(qnt, datetime.fromisoformat(insert_data[qnt - 1][0])))
        inserted_data_qnt += qnt

    # delete data which was processed by another procedure 
    try:
        cursor.execute("delete from public.list_tenders where is_load_data = true")
        cursor.execute("reindex table public.list_tenders")
    except Exception as e:
        sendTelegramMessage("Delete and reindex went wrong: {}".format(str(e)))
    sendTelegramMessage("Huuhhh, it was a hard work, which has taken {}\nInserted {} tenders".format(count_total_time(start_time, datetime.now()), inserted_data_qnt))

    conn.close()


