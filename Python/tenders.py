import psycopg2
from datetime import datetime
import time
import json 
import requests

# function to count taken time for running the process
def add_0_to_num(num):
    return "0" + str(int(num)) if num <= 9 else str(int(num))

def time_taken(start):
    end = datetime.now() 
    time_difference = (end - start).total_seconds()
    minutes, seconds = divmod(time_difference, 60)
    hours, minutes = divmod(minutes, 60)

    time_int = [add_0_to_num(val) for val in [hours, minutes, seconds]]
    return "{}:{}:{}".format(time_int[0], time_int[1], time_int[2])

# time builder
def time_taken2(seconds):
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    time_int = [add_0_to_num(val) for val in [hours, minutes, seconds]]
    return "{}:{}:{}".format(time_int[0], time_int[1], time_int[2])

# function to send messages to telegram channel
def sendTelegramMessage(message):
    token = "enter_your_token"
    chat_id = 'enter_your_chat'
    apiURL = 'https://api.telegram.org/bot{}/sendMessage'.format(token)

    request = requests.post(url = apiURL, json = {"chat_id": chat_id, "text": "TENDERS" + "\n" + message})
    print(request)
    if not json.loads(request.text)["ok"]:
        requests.post(url = apiURL, json = {"chat_id": chat_id, "text": "TENDERS" + "\n" + json.loads(request.text)["description"]})

# function to take 100001 cuurently unloaded rows from list_tenders ordered by date_modified ascending
def get_data_to_process(cursor):
    query = """ select
                    tender_id
                from
                (
                    select
                        tender_id
                    from public.list_tenders
                    where is_load_data = false
                    order by date_modified
                    limit 100001
                ) sub 
                group by 
                    tender_id"""
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        sendTelegramMessage("Error occured while getting data to process: {}".format(str(e)))
        return None

# class Tenders which performs all the operations for the process 
class Tenders:
    # operational variables 
    api_time = 0
    insert_function_time = 0
    tenders_to_insert = []
    items_to_insert = []
    deleted_from_list_tenders = 0
    # queries to delete, insert, count and update
    query_delete_tenders_or_items = """delete from {}
                                        where tender_id in (
                                                            select 
                                                                sub.tender_id
                                                            from
                                                            (
                                                                values 
                                                                (%s)
                                                            ) sub (tender_id)
                                                           )
                                        and batch_timestamp {} {};
                                    """
    query_insert_tender_items = """insert into public.tender_items
                                    (
                                        tender_id,
                                        classification_id,
                                        batch_timestamp
                                    )
                                    values
                                    (%s, %s, {});"""
    
    query_insert_tenders = """INSERT INTO public.tenders
                      (
                        tender_id, 
                        tender_id_code, 
                        identifier_edr_id, 
                        date_of_publication, 
                        date_modified, 
                        inserted_at, 
                        is_processed,
                        batch_timestamp
                      )
                        VALUES (%s, %s, %s, %s, %s, now(), false, {});"""
    query_count_list_tenders = """select 
                                        count(id) as qnt
                                    from public.list_tenders 
                                    where tender_id in (
                                                            select 
                                                                sub.tender_id
                                                            from 
                                                            (
                                                                values
                                                                {}
                                                            ) sub (tender_id)
                                                            where sub.tender_id is not null
                                                        )
                                """
    query_update_list_tenders = """update public.list_tenders
                                   set is_load_data = true
                                   where tender_id in (
                                                        select 
                                                            sub.tender_id
                                                        from 
                                                        (
                                                            values
                                                            (%s)
                                                        ) sub (tender_id)
                                                        where sub.tender_id is not null
                                                      )"""
    
    # init function which takes the data for processing, assigns batch_timestamp and starts the main process 
    def __init__(self, cursor, data_to_process):
        self.cursor = cursor
        self.data_to_process_original = data_to_process
        self.data_to_process = [tender[0] for tender in data_to_process]
        self.batch_timestamp = time.time()
        sendTelegramMessage("Batch_timestamp for this process: {}".format(self.batch_timestamp))
        self.process_data()

    # function to count the data in list_tenders which will be updated
    def count_list_tenders(self):
        try:
            tender_ids_list = ["('{}')".format(tender[0]) for tender in self.tenders_to_insert]
            tedner_ids_str = ",".join(tender_ids_list)

            self.cursor.execute(self.query_count_list_tenders.format(tedner_ids_str))
            self.deleted_from_list_tenders = cursor.fetchall()[0][0]
        except Exception as e:
            sendTelegramMessage("Error occured while counting data to delete from list_tenders: {}".format(str(e)))

    # function to process the tender_ids by getting more information from the api and inserting it to the other tables
    def process_data(self):
        ret = True

        for tender_id in self.data_to_process:
            #print(tender_id)
            try:
                # request part
                start_time = datetime.now()
                request = requests.get("https://public.api.openprocurement.org/api/2.5/tenders/{}".format(tender_id))
                tender_api_data = json.loads(request.text)['data']
                self.api_time += (datetime.now() - start_time).total_seconds()
                # processing tenders
                tender_api_data_id = tender_api_data['id']
                tender = {
                    'tender_id': tender_api_data_id,
                    'tender_id_code': tender_api_data['tenderID'],
                    'identifier_edr_id': tender_api_data['procuringEntity']['identifier']['id'],
                    'date_of_publication': str(datetime.date(datetime.fromisoformat(tender_api_data['dateCreated']))),
                    'date_modified': str(datetime.fromisoformat(tender_api_data['dateModified']))
                }
                #print(tender)
                self.tenders_to_insert.append(tuple(tender.values()))
                
                # processing items
                for item in tender_api_data['items']:
                    item = {
                        'tender_id': tender_api_data_id,
                        'classification_id': item['classification']['id']
                    }
                    #print(item)
                    self.items_to_insert.append(tuple(item.values()))

                # if more than 5000 - insert
                if len(self.tenders_to_insert) >= 10000:
                    ret = self.insert()
                    self.tenders_to_insert = []
                    self.items_to_insert = []
                    if not ret:
                        break

            except Exception as e:
                sendTelegramMessage("Error occured while requesting API or processing data: {}.".format(str(e)))
        
        # if some data is left (less than 5000) - it should be left
        if len(self.tenders_to_insert) or len(self.items_to_insert):
            ret = self.insert()

        # was the process successful
        if ret:
            some_stats = "\nTime taken for api processing: {}\nTime taken for insert operations: {}".format(time_taken2(self.api_time), time_taken2(self.insert_function_time))
            sendTelegramMessage("Process has ended successfully.\nTime taken for the process = {}.{}\nCheck data by the batch_timestamp = {} (some data can be deleted by this moment)".format(time_taken(start), some_stats, self.batch_timestamp))
        else:
            sendTelegramMessage("Looks like some data hasn't been inserted.\nRead previous messages to get more details")

    # delete function 
    def delete(self, table, this_data = False):
        def delete_this_data():
            try:
                self.cursor.executemany(self.query_delete_tenders_or_items.format("public.tenders", "=",self.batch_timestamp), [(tender[0], ) for tender in self.tenders_to_insert])
                self.cursor.executemany(self.query_delete_tenders_or_items.format("public.tender_items", "=", self.batch_timestamp), [(tender[0], ) for tender in self.items_to_insert])
                sendTelegramMessage("Deleted all loaded data of this batch: {}".format(self.batch_timestamp))
            except Exception as e:
                sendTelegramMessage("Deleting all loaded data of this batch ({}) has crashed: {}".format(self.batch_timestamp, str(e)))

        if this_data == False:
            if table == 'T': 
                try:
                    #print(self.data_to_process_original)
                    self.cursor.executemany(self.query_delete_tenders_or_items.format("public.tenders", "<>",self.batch_timestamp), [(tender[0], ) for tender in self.tenders_to_insert])
                    return 1
                except Exception as e:
                    sendTelegramMessage("Error occured while deleting previous tenders: {}.\nTrying to delete all loaded data for this batch".format(str(e)))
                    delete_this_data()
                    return 0

            elif table == 'I':
                try:
                    self.cursor.executemany(self.query_delete_tenders_or_items.format("public.tender_items", "<>", self.batch_timestamp), [(tender[0], ) for tender in self.items_to_insert])
                    return 1
                except Exception as e:
                    sendTelegramMessage("Error occured while deleting previous tender items: {}\n.Trying to delete all loaded data for this batch".format(str(e)))
                    delete_this_data()
                    return 0
            
        elif this_data == True:
            delete_this_data()

    def update(self):
        self.count_list_tenders()
        try:
            self.cursor.executemany(self.query_update_list_tenders, [(tender[0], ) for tender in self.tenders_to_insert])
            sendTelegramMessage("Updated status of {} tenders in list_tenders".format(self.deleted_from_list_tenders))
            return 1
        except Exception as e:
            sendTelegramMessage("Error occured while updating list_tenders: {}.\nTrying to delete all loaded data for this batch".format(str(e)))
            self.delete('', this_data = True)
            return 0
    
    def insert(self):
        start_time = datetime.now()
        can_process_further = False
        try:
            #print("Inserting tenders")
            self.cursor.executemany(self.query_insert_tenders.format(self.batch_timestamp), self.tenders_to_insert)
            can_process_further = True
            sendTelegramMessage("Inserted {} processed tenders.\nLast tenderID is {}".format(len(self.tenders_to_insert), self.tenders_to_insert[-1][1]))
        except Exception as e:
            sendTelegramMessage("Error occured while inserting tenders: {}".format(str(e)))

        if can_process_further:
            try:
                #print("Inserting items")
                self.cursor.executemany(self.query_insert_tender_items.format(self.batch_timestamp), self.items_to_insert)
                sendTelegramMessage("Inserted {} processed items".format(len(self.items_to_insert)))
            except Exception as e:
                sendTelegramMessage("Error occured while inserting items: {}".format(str(e)))
                can_process_further = False

            if can_process_further:
                delete_t = self.delete('T')
                if delete_t:
                    delete_i = self.delete('I')
                    if delete_i:
                        upd = self.update()
                        self.insert_function_time += (datetime.now() - start_time).total_seconds()
                        return upd
                
        return None

        


conn_str = "host = 'localhost' dbname = 'prozzoro_load_data' user = 'postgres' password = 'password' port = 5432"
is_connected = False
try:
    conn = psycopg2.connect(conn_str)
    conn.autocommit = True
    cursor = conn.cursor()
    is_connected = True
except Exception as e:
    sendTelegramMessage("Oopppssss, error occured while creating a connection to database: {}".format(str(e)))

# main
start = datetime.now()

if is_connected:
    sendTelegramMessage("Process has started at {}".format(start.strftime('%d/%m/%Y %H:%M:%S')))
    data_to_process = get_data_to_process(cursor = cursor)
    if data_to_process:
        tenders = Tenders(cursor = cursor, data_to_process = data_to_process)
    else:
        sendTelegramMessage("No data to process...")

    conn.close()
