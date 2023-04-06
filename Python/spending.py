import psycopg2 
import requests 
import json
from datetime import datetime 
import pandas as pd

## telegram bot 
def sendTelegramMessage(message):
    token = "enter_your_token"
    chat_id = 'enter_your_chat'
    apiURL = 'https://api.telegram.org/bot{}/sendMessage'.format(token)

    response = requests.post(apiURL, json= {"chat_id": chat_id, "text": message})
    print(response.text)

## def millisecond beautifier 
def convert_from_milliseconds(ms):
    seconds, ms = divmod(ms, 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    return (str(int(hours)) if hours > 9 else "0" + str(int(hours))) + ":" + (str(int(minutes)) if minutes > 9 else "0" + str(int(minutes))) + ":" + (str(int(seconds)) if seconds > 9 else "0" + str(int(seconds))) + "." + str("000" + str(int(ms % 1000)))[-3:]

## connect
try:
    ## orm_connection
    conn_str_orm = "host='localhost' dbname='RebuildUkraine_General_Orm' user='postgres' password='password' port=5432"
    conn_orm = psycopg2.connect(conn_str_orm)
    ## declare cursor object
    cursor_orm = conn_orm.cursor()

    ##spending connection 
    conn_str_spending = "host='localhost' dbname='spending' user='postgres' password='password' port=5432"
    conn_spending = psycopg2.connect(conn_str_orm)
    ## declare cursor object
    cursor_spending = conn_orm.cursor()
except Exception as e:
    sendTelegramMessage("Connection to db failed: " + str(e))

## DECLARATION OF VARIABLES AND FUNCTIONS 

## declare object to save statistics
time_statistic = {
    "started": str(datetime.now()),
    "db_IDs": None,
    "EDRPOUs": None,
    "api_logic_insert_time": {
        "document_info": [0, 0, 0],
        "acts_info": [0, 0, 0],
        "addendums_info": [0, 0, 0],
        "transactions_info": [0, 0, 0]
    },
    "document_info_inserted": 0,
    "acts_info_inserted": 0,
    "addendums_info_inserted": 0,
    "transactions_info_inserted": 0,
    "contractors_inserted": 0,
    "ended": None
}

## declare function to insert data from api 
def insert_api_data(table_name, cursor, conn, data):
    table_query = {
        "document_info": """INSERT INTO public.document_info
                            (
                                id, 
                                edrpou, 
                                documentnumber, 
                                documentdate, 
                                signdate, 
                                signature, 
                                amount, 
                                currency, 
                                currencyamountuah, 
                                fromdate, 
                                todate, 
                                subject, 
                                noterm, 
                                pdvinclude,
                                pdvamount, 
                                tender, 
                                reason, 
                                specifications, 
                                iscpvvat, 
                                procurementitems, 
                                idtenderprozorro,
                                created_at
                            )
                                VALUES (%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, now())""",
        "acts_info": """INSERT INTO public.acts_info
                        (
                            id, 
                            edrpou, 
                            documentnumber, 
                            documentdate, 
                            signdate, 
                            signature, 
                            amount, 
                            currency, 
                            currencyamountuah, 
                            contractors, 
                            parentid, 
                            pdvinclude, 
                            pdvamount, 
                            specifications, 
                            iscpvvat, 
                            procurementitems, 
                            created_at
                        )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())""",
        "addendums_info": """INSERT INTO public.addendums_info
                            (
                                id, 
                                edrpou, 
                                documentnumber, 
                                documentdate, 
                                signdate, 
                                signature, 
                                amount, 
                                currency, 
                                currencyamountuah, 
                                contractors, 
                                parentid, 
                                fromdate, 
                                todate, 
                                noterm, 
                                subject, 
                                amountincrease, 
                                pdvinclude, 
                                pdvamount, 
                                correctiontype, 
                                iscorrectionwithpdf, 
                                correctionvatvalue, 
                                reasontypes, 
                                reasonothercomment, 
                                specifications, 
                                iscpvvat, 
                                procurementitems, 
                                created_at
                            )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())""",
        "transactions_info": """INSERT INTO public.transactions_info
                                (
                                    id, 
                                    doc_vob, 
                                    doc_vob_name, 
                                    doc_number, 
                                    doc_date, 
                                    doc_v_date, 
                                    trans_date, 
                                    amount, 
                                    amount_cop, 
                                    currency, 
                                    payer_edrpou, 
                                    payer_name, 
                                    payer_account, 
                                    payer_mfo, 
                                    payer_bank, 
                                    recipt_edrpou, 
                                    recipt_name, 
                                    recipt_account, 
                                    recipt_bank, 
                                    recipt_mfo, 
                                    payment_details, 
                                    doc_add_attr, 
                                    region_id, 
                                    payment_type, 
                                    payment_data, 
                                    source_id, 
                                    source_name, 
                                    kekv, 
                                    kpk, 
                                    contractid, 
                                    contractnumber, 
                                    budgetcode, 
                                    created_at
                                )
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())""" ,
        "contractors": """INSERT INTO public.contractors
                          (
                            document_info_id, 
                            name, 
                            edrpou, 
                            contractortype, 
                            firstname, 
                            lastname, 
                            middlename, 
                            address,
                            created_at
                           )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())"""
    }

    start_time = datetime.now()
    try:
        cursor.executemany(table_query[table_name], data)
        conn.commit()
    except Exception as e:
        sendTelegramMessage("Insert into " + table_name + " failed: " + str(e))
    # save statistic inside the function
    if table_name != 'contractors':
        time_statistic["api_logic_insert_time"][table_name][2] += (datetime.now() - start_time).total_seconds() * 1000
    time_statistic[table_name + "_inserted"] += len(data)

sendTelegramMessage("Here we go, starting at " + str(datetime.now()))

## declare lists of old IDS and function to insert data into them
def get_data_db_IDs(table_name, cursor, conn):
    data_query = []

    table_query = {
        "document_info": "select id from public.document_info",
        "acts_info": "select id from public.acts_info",
        "addendums_info": "select id from public.addendums_info",
        "transactions_info": "select id from public.transactions_info",
        "contractors": "select document_info_id, edrpou from public.contractors"
    }

    try:
        cursor.execute(table_query[table_name])
        data_query = cursor.fetchall()
        conn.commit()
    except Exception as e:
        sendTelegramMessage("Getting old IDs of " + table_name + " failed: " + str(e))
    return [data[0] for data in data_query] if table_name != 'contractors' else list(data_query)


doc_info_db_IDs = get_data_db_IDs("document_info", cursor_spending, conn_spending)
acts_info_db_IDs = get_data_db_IDs("acts_info", cursor_spending, conn_spending)
addendums_info_db_IDs = get_data_db_IDs("addendums_info", cursor_spending, conn_spending)
transact_info_db_IDs = get_data_db_IDs("transactions_info", cursor_spending, conn_spending)
contractors_db_Ids = get_data_db_IDs("contractors", cursor_spending, conn_spending)

## save statistic
time_statistic["db_IDs"] = str(datetime.now())

## get EDRPOUs
try:
    cursor_orm.execute("""select 
        customers.orgEDRPOU as customerEDRPOU,
        contractors.orgEDRPOU as contractorEDRPOU
    from 
    (
        select 
            "contractId" as contrId,
            org."EDRPOU" as orgEDRPOU
        from public.contract_side cs
        inner join public.organization org on cs."OrganizationId" = org.Id 
        where "sideTypeId" = 1
    ) customers 
    inner join 
    (
        select 
            "contractId" as contrId,
            org."EDRPOU" as orgEDRPOU
        from public.contract_side cs
        inner join public.organization org on cs."OrganizationId" = org.Id 
        where "sideTypeId" = 2
    ) contractors on customers.contrId = contractors.contrId
    group by
        customers.orgEDRPOU,
        contractors.orgEDRPOU
    """)

    EDRPOU = cursor_orm.fetchall()
except Exception as e:
    sendTelegramMessage("Getting EDRPOUs failed: " + str(e))

conn_orm.commit()
conn_orm.close()

## save statistic
time_statistic["EDRPOUs"] = str(datetime.now())

## MAIN
if len(EDRPOU):
    current_date = datetime.now().date()
    prev2mon_date = (pd.to_datetime(current_date) + pd.DateOffset(months=-2)).date()

    urlContracts = 'http://api.spending.gov.ua/api/v2/disposers/contracts?disposerId={}&contractorId={}'
    urlActs = 'http://api.spending.gov.ua/api/v2/disposers/acts?disposerId={}&contractorId={}'
    urlAddenDums = 'http://api.spending.gov.ua/api/v2/disposers/addendums?disposerId={}&contractorId={}'
    urlTransactions = 'http://api.spending.gov.ua/api/v2/api/transactions/?payers_edrpous={}&recipt_edrpous={}&startdate={}&enddate={}'

    for cust_contr in EDRPOU:
        if cust_contr[0] and cust_contr[1]:
            ## contracts api
            contr_time_start = datetime.now()
            try:
                contracts_request = requests.get(urlContracts.format(cust_contr[0], cust_contr[1]))
                contracts_data = json.loads(contracts_request.text)["documents"]
            except Exception as e:
                sendTelegramMessage("Contract request failed: " + str(e) + '.\nRequest is: ' + urlContracts.format(cust_contr[0], cust_contr[1]) + '.\nData is: ' + str(cust_contr))
            time_statistic["api_logic_insert_time"]["document_info"][0] += (datetime.now() - contr_time_start).total_seconds() * 1000
            ## acts api
            acts_time_start = datetime.now()
            try:
                acts_request = requests.get(urlActs.format(cust_contr[0], cust_contr[1]))
                acts_data = json.loads(acts_request.text)["documents"]
            except Exception as e:
                sendTelegramMessage("Act request failed: " + str(e))
            time_statistic["api_logic_insert_time"]["acts_info"][0] += (datetime.now() - acts_time_start).total_seconds() * 1000
            ##addendums api
            dums_time_start = datetime.now()
            try:
                addendums_request = requests.get(urlAddenDums.format(cust_contr[0], cust_contr[1]))
                addendums_data = json.loads(addendums_request.text)["documents"]
            except Exception as e:
                sendTelegramMessage("Addendum request failed: " + str(e))
            time_statistic["api_logic_insert_time"]["addendums_info"][0] += (datetime.now() - dums_time_start).total_seconds() * 1000
            ##transactions api
            transact_time_start = datetime.now()
            try:
                transactions_request = requests.get(urlTransactions.format(cust_contr[0], cust_contr[1], prev2mon_date, current_date,))
                transactions_data = json.loads(transactions_request.text)
            except Exception as e:
                sendTelegramMessage("Transaction request failed: " + str(e))
            time_statistic["api_logic_insert_time"]["transactions_info"][0] += (datetime.now() - transact_time_start).total_seconds() * 1000

        ## contract proccessing
        if contracts_request.status_code == 200:
            contract_insert_data = []
            contractors_insert_data = []
            contr_time_start = datetime.now()
            for c in contracts_data:
                if c["id"] not in doc_info_db_IDs:
                    try:
                        ## contract itself
                        contract_insert_data.append((c["id"], c["edrpou"], c["documentNumber"], c["documentDate"], c["signDate"], json.dumps(c['signature'], ensure_ascii = False), c["amount"],
                                                    c["currency"], c["currencyAmountUAH"], c["fromDate"], c["toDate"],
                                                    c["subject"], c["noTerm"], c["pdvInclude"], c["pdvAmount"], c["tender"], c["reason"],
                                                    json.dumps(c['specifications'], ensure_ascii = False), c['isCpvVat'],
                                                    json.dumps(c['procurementItems'], ensure_ascii = False), c['idTenderProzorro']))
                        doc_info_db_IDs.append(c["id"])
                        ##contractors 
                        for contractor in c['contractors']:
                            if (c['id'], contractor['identifier']) not in contractors_db_Ids:
                                contractor_template = {
                                    'document_info_id': c["id"], 
                                    'name': contractor['name'], 
                                    'identifier': contractor['identifier'], 
                                    'contractorType': contractor['contractorType'], 
                                    'firstName': contractor['firstName'], 
                                    'lastName': contractor['lastName'], 
                                    'middleName': contractor['middleName'], 
                                    'address': json.dumps(contractor['address'], ensure_ascii = False)
                                }
                                contractors_db_Ids.append((c['id'], contractor['identifier']))
                                contractors_insert_data.append(tuple(contractor_template.values()))

                    except Exception as e:
                        sendTelegramMessage(str(e) + "\n" + str(c))
                    
            time_statistic["api_logic_insert_time"]["document_info"][1] += (datetime.now() - contr_time_start).total_seconds() * 1000
            insert_api_data("document_info", cursor_spending, conn_spending, contract_insert_data)
            insert_api_data("contractors", cursor_spending, conn_spending, contractors_insert_data)
        else:
            sendTelegramMessage("Request: " + urlContracts.format(cust_contr[0], cust_contr[1]) + " gave response " + str(contracts_request.status_code))

        ##acts proccessing 
        if acts_request.status_code == 200:
            act_insert_data = []
            act_time_start = datetime.now()
            for act in acts_data:
                if act["id"] not in acts_info_db_IDs and act["parentId"] in doc_info_db_IDs:
                    try:
                        act_insert_data.append((act["id"], act["edrpou"], act["documentNumber"], act["documentDate"], act["signDate"], json.dumps(act["signature"], ensure_ascii = False), 
                                                act["amount"], act["currency"], act["currencyAmountUAH"], json.dumps(act["contractors"], ensure_ascii = False), act["parentId"],
                                                act["pdvInclude"], act["pdvAmount"], json.dumps(act["specifications"], ensure_ascii = False), act["isCpvVat"],
                                                json.dumps(act["procurementItems"], ensure_ascii = False)))
                        acts_info_db_IDs.append(act["id"])
                    except Exception as e:
                        sendTelegramMessage(str(e) + "\n" + str(act))

            time_statistic["api_logic_insert_time"]["acts_info"][1] = (datetime.now() - act_time_start).total_seconds() * 1000
            insert_api_data("acts_info", cursor_spending, conn_spending, act_insert_data)
        else: 
            sendTelegramMessage("Request: " + urlActs.format(cust_contr[0], cust_contr[1]) + " gave response " + str(acts_request.status_code))

        ## addendums proccessing 
        if addendums_request.status_code == 200:
            dum_insert_data = []
            dum_time_start = datetime.now()
            for dum in addendums_data:
                if dum["id"] not in addendums_info_db_IDs and dum["parentId"] in doc_info_db_IDs:
                    try:
                        dum_insert_data.append((dum["id"], dum["edrpou"], dum["documentNumber"], dum["documentDate"], dum["signDate"], json.dumps(dum["signature"], ensure_ascii = False),
                                                dum["amount"], dum["currency"], dum["currencyAmountUAH"], json.dumps(dum["contractors"], ensure_ascii = False),
                                                dum["parentId"], dum["fromDate"], dum["toDate"], dum["noTerm"], dum["subject"], dum["amountIncrease"], dum["pdvInclude"],
                                                dum["pdvAmount"], dum["correctionType"], dum["isCorrectionWithPDF"], dum["correctionVATValue"], json.dumps(dum["reasonTypes"], ensure_ascii = False),
                                                dum["reasonOtherComment"], json.dumps(dum["specifications"], ensure_ascii = False), dum["isCpvVat"], 
                                                json.dumps(dum["procurementItems"], ensure_ascii = False)))
                        addendums_info_db_IDs.append(dum["id"])
                    except Exception as e:
                        sendTelegramMessage(str(e) + "\n" + str(dum))

            time_statistic["api_logic_insert_time"]["addendums_info"][1] = (datetime.now() - dum_time_start).total_seconds() * 1000
            insert_api_data("addendums_info", cursor_spending, conn_spending, dum_insert_data)
        else: 
            sendTelegramMessage("Request: " + urlAddenDums.format(cust_contr[0], cust_contr[1]) + " gave response " + str(addendums_request.status_code))

        ## transactions proccessing 
        if transactions_request.status_code == 200:
            transact_insert_data = []
            transact_time_start = datetime.now()
            for tr in transactions_data:
                if tr["id"] not in transact_info_db_IDs:
                    try:
                        transact_insert_data.append((tr["id"], tr["doc_vob"], tr["doc_vob_name"], tr["doc_number"], tr["doc_date"], tr["doc_v_date"], 
                                                    tr["trans_date"], tr["amount"], tr["amount_cop"], tr["currency"], tr["payer_edrpou"], tr["payer_name"], tr["payer_account"],
                                                    tr["payer_mfo"], tr["payer_bank"], tr["recipt_edrpou"], tr["recipt_name"], tr["recipt_account"], tr["recipt_bank"], tr["recipt_mfo"],
                                                    tr["payment_details"], tr["doc_add_attr"], tr["region_id"], tr["payment_type"], json.dumps(tr["payment_data"], ensure_ascii = False), tr["source_id"],
                                                    tr["source_name"], tr["kekv"], tr["kpk"], tr["contractId"], tr["contractNumber"], tr["budgetCode"]))
                        transact_info_db_IDs.append(tr["id"])
                    except Exception as e:
                        sendTelegramMessage(str(e) + "\n" + str(tr))
                    
            time_statistic["api_logic_insert_time"]["transactions_info"][1] = (datetime.now() - transact_time_start).total_seconds() * 1000
            insert_api_data("transactions_info", cursor_spending, conn_spending, transact_insert_data)
        else:
            sendTelegramMessage("Request: " + urlTransactions.format(cust_contr[0], cust_contr[1], prev2mon_date, current_date) + " gave response " + str(transactions_request.status_code))
## finally end     
conn_spending.commit()
            
conn_spending.close()

time_statistic["ended"] = str(datetime.now())
 
## build message 
message1 = "Pipeline started at " + time_statistic["started"] + ".\nOld IDs from database were selected at " + time_statistic["db_IDs"]  + ".\n" + "EDRPOUs were selected at " + time_statistic["EDRPOUs"] + ".\n"
message2 = "Time taken for processing document_info: \n" + "\tapi:\t" + convert_from_milliseconds(time_statistic["api_logic_insert_time"]["document_info"][0]) + "\n\tlogic:\t" + convert_from_milliseconds(time_statistic["api_logic_insert_time"]["document_info"][1]) + "\n\tinsert:\t" + convert_from_milliseconds(time_statistic["api_logic_insert_time"]["document_info"][2])
message3 = "\nTime taken for processing acts_info: \n" + "\tapi:\t" + convert_from_milliseconds(time_statistic["api_logic_insert_time"]["acts_info"][0]) + "\n\tlogic:\t" + convert_from_milliseconds(time_statistic["api_logic_insert_time"]["acts_info"][1]) + "\n\tinsert:\t" + convert_from_milliseconds(time_statistic["api_logic_insert_time"]["acts_info"][2])
message4 = "\nTime taken for processing addendums_info: \n" + "\tapi:\t" + convert_from_milliseconds(time_statistic["api_logic_insert_time"]["addendums_info"][0]) + "\n\tlogic:\t" + convert_from_milliseconds(time_statistic["api_logic_insert_time"]["addendums_info"][1]) + "\n\tinsert:\t" + convert_from_milliseconds(time_statistic["api_logic_insert_time"]["addendums_info"][2])
message5 = "\nTime taken for processing transactions_info: \n" + "\tapi:\t" + convert_from_milliseconds(time_statistic["api_logic_insert_time"]["transactions_info"][0]) + "\n\tlogic:\t" + convert_from_milliseconds(time_statistic["api_logic_insert_time"]["transactions_info"][1]) + "\n\tinsert:\t" + convert_from_milliseconds(time_statistic["api_logic_insert_time"]["transactions_info"][2])
message6 = "\nDocuments inserted: " + str(time_statistic["document_info_inserted"]) + "\nActs inserted: " + str(time_statistic["acts_info_inserted"]) + "\nAddendums inserted: " + str(time_statistic["addendums_info_inserted"]) + "\nTransactions inserted: "  + str(time_statistic["transactions_info_inserted"])
message7 = "\nJob ended at: " + time_statistic["ended"]

message = message1 + message2 + message3 + message4 + message5 + message6 + message7


sendTelegramMessage(message)





    

