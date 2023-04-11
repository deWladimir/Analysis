import psycopg2
import json
import requests
from datetime import datetime

# Dictionary to count statistics
TimeDict = {
    'API': 0,
    'DB': 0,
    'Processed': 0,
    'Start': datetime.now()
}

# Functions to count taken time in hh:mm:ss format
def add0ToNum(value):
    return "0" + str(int(value)) if int(value) <= 9 else str(int(value)) 

def timeBeautifier(timeDiff):
    minutes, seconds  = divmod(timeDiff, 60)
    hours, minutes = divmod(minutes, 60)

    res = []
    for value in [seconds, minutes, hours]:
        res.append(add0ToNum(value))
    
    return "{}:{}:{}".format(res[2], res[1], res[0])


# Function to send a telegram message
def sendTelegramMessage(message):
    token = "your_token"
    chat_id = 'your_chat_id'
    apiURL = 'https://api.telegram.org/bot{}/sendMessage'.format(token)

    try:
        requests.post(apiURL, json = {"chat_id": chat_id, "text": "TARGET_TENDERS\n" + message})
    except Exception as e:
        print("Sending Telegram message has failed: {}".format(str(e)))

# Function to extract from the database necessary EDRPOUs
def getCustomerEDRPOUs():
    ReUkrConnStr = "host = 'localhost' dbname = 'RebuildUkraine_General_Orm' user = 'postgres' password = 'password' port  = 5432"

    try:
        ReUkrConn = psycopg2.connect(ReUkrConnStr)
        ReUkrConn.autocommit = True
        ReUkrCursor = ReUkrConn.cursor()
    except Exception as e:
        sendTelegramMessage("Connection to 'RebuildUkraine_General_Orm' has failed: {}".format(str(e)))
        return []

    resultEDRPOUs = []
    queryEDRPOUs  = """
                        select 
                            org."EDRPOU"
                        from public.organization_role_type_organization role_type
                        inner join public.organization org on role_type."organizationId" = org.Id
                        where "organizationRoleTypeId" = 1
                        group by 
                            org."EDRPOU"
                    """
    
    try:
        ReUkrCursor.execute(queryEDRPOUs)
        resultEDRPOUs = ReUkrCursor.fetchall()
    except Exception as e:
        sendTelegramMessage("Executing EDRPOU query has failed: {}".format(str(e)))
        return []
    
    try:
        ReUkrConn.close()
    except Exception as e:
        sendTelegramMessage("Closing connection to 'RebuildUkraine_General_Orm' has failed: {}".format(str(e)))
    
    return [EDRPOU[0] for EDRPOU in resultEDRPOUs]


# Function to extract necessary tenders from the source database
def getTendersFromSource(EDRPOU):
    ProzorroSourceConnStr = "host = 'localhost' dbname = 'RebuildUkraine_Prozorro_Source_Data' user = 'postgres' password = 'password' port = 5432"

    try:
        ProzzoroSourceConn = psycopg2.connect(ProzorroSourceConnStr)
        ProzzoroSourceConn.autocommit = True
        ProzzoroSourceCursor  = ProzzoroSourceConn.cursor()
    except Exception as e:
        sendTelegramMessage("Connection to 'RebuildUkraine_Prozorro_Source_Data' has failed: {}.\nEDRPOU = '{}'".format(str(e), EDRPOU))
        return []
    
    Tenders = []
    queryTenders =  """
                        select 
                            tender_id
                        from public.tenders 
                        where is_processed = false
                        and identifier_edr_id = '{}'
                        order by date_modified
                        limit 10000
                    """.format(EDRPOU)

    #print(queryTenders)

    try:
        _Start = datetime.now()
        ProzzoroSourceCursor.execute(queryTenders)
        Tenders = ProzzoroSourceCursor.fetchall()
        TimeDict['DB'] += (datetime.now() - _Start).total_seconds()
    except Exception as e:
        sendTelegramMessage("Executing query to get SourceTenders has failed: {}".format(str(e)))
        return []
    
    try:
        ProzzoroSourceConn.close()
    except Exception as e:
        sendTelegramMessage("Closing connection to 'RebuildUkraine_Prozorro_Source_Data' has failed: {}".format(str(e)))

    return [Tender[0] for Tender in Tenders]

# Class to operate tenders 
class TargetTender:
    # Connection strings
    _TargetConnectionStr = "host = 'localhost' dbname = 'RebuildUkraine_Prozorro_Target_Data' user = 'postgres' password = 'password' port = 5432"
    _SourceConnectionStr = "host = 'localhost' dbname = 'RebuildUkraine_Prozorro_Source_Data' user = 'postgres' password = 'password' port = 5432"

    # Queries to delete|insert in TargetDB and update in SourceDB
    _ProcessTargetQuery = """
                            delete from public.tenders 
                            where tender_id = '{}';

                            INSERT INTO public.tenders
                            (
                                tender_id, 
                                "EDRPOU", 
                                "tenderID_Code", 
                                title, 
                                value_amount, 
                                "DateOfPublication", 
                                "tenderPeriod_startDate", 
                                "tenderPeriod_endDate", 
                                "lotCount", 
                                "contractCount", 
                                "originalData"
                            )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                          """
    
    _ProcessSourceQuery = """
                            update public.tenders 
                            set is_processed = true
                            where tender_id = '{}';
                          """
    
    # init function
    def __init__(self, data, EDRPOU):
        # Get data and EDRPOU
        self._SourceTenders = data
        self._EDRPOU = EDRPOU

        # Connect to source and target
        try:
            self._SourceConnection = psycopg2.connect(self._SourceConnectionStr)
            self._SourceConnection.autocommit = True
            self._SourceCursor = self._SourceConnection.cursor()

            self._TagretConnection = psycopg2.connect(self._TargetConnectionStr)
            self._TagretConnection.autocommit = True
            self._TargetCursor = self._TagretConnection.cursor()

            self.GoOn = True
        except Exception as e:
            sendTelegramMessage("Connection to Source or Tagret database has failed: {}".format(str(e)))
            self.GoOn = False

        # If connected, start processing
        if self.GoOn:
            self._Processed = 0
            self.ProcessTenders()
            sendTelegramMessage("For EDRPOU = '{}' {} tenders have been processed.".format(self._EDRPOU, self._Processed))
            TimeDict['Processed'] += self._Processed
        else:
            sendTelegramMessage("Problems with processing data for EDRPOU: '{}' have happened.\nSee previous messages".format(self._EDRPOU))

    # Processing function
    def ProcessTenders(self):
        # Iterate the list of tender_id
        for SourceTender in self._SourceTenders:
            try:
                # Get info from api
                _Start = datetime.now()
                TenderInfo = json.loads(requests.get('https://public.api.openprocurement.org/api/2.5/tenders/{}'.format(SourceTender)).text)['data']
                TimeDict['API'] += (datetime.now() - _Start).total_seconds()
            except Exception as e:
                sendTelegramMessage("Requesting data for Tender: '{}' has failed.\n{}".format(SourceTender, str(e)))
                continue
            
            try:
                # Build dictionary object with necessary fields
                InsertTenderDict = {
                    'tender_id': SourceTender,
                    'EDRPOU': TenderInfo['procuringEntity']['identifier']['id'],
                    'tenderID_Code': TenderInfo['tenderID'],
                    'title': TenderInfo.get('title'),
                    'value_amount': TenderInfo.get('value').get('amount') if TenderInfo.get('value') else None,
                    'DateOfPublication': TenderInfo['tenderID'][3:13],
                    'tenderPeriod_startDate': TenderInfo.get('tenderPeriod').get('startDate') if TenderInfo.get('tenderPeriod') else None,
                    'tenderPeriod_endDate': TenderInfo.get('tenderPeriod').get('endDate') if TenderInfo.get('tenderPeriod') else None,
                    'lotCount': len(TenderInfo.get('lots')) if TenderInfo.get('lots') else 0,
                    'contractCount': len(TenderInfo.get('contracts')) if TenderInfo.get('contracts') else 0,
                    'originalData': json.dumps(TenderInfo, ensure_ascii=False)
                }
            except Exception as e:
                sendTelegramMessage("Error while parsing request data to dictionary object: {}.\nTender_Id = '{}'".format(str(e), SourceTender))
                continue
            
            _Start = datetime.now()
            # Delete and insert data in TargetDB
            try:
                self._TargetCursor.execute(self._ProcessTargetQuery.format(SourceTender), tuple(InsertTenderDict.values()))
            except Exception as e:
                sendTelegramMessage("Error while deleting/inserting data: {}.\nTender_Id = '{}'".format(str(e), SourceTender))
                continue
            
            # Update data in SourceDB
            try:
                self._SourceCursor.execute(self._ProcessSourceQuery.format(SourceTender))
            except Exception as e:
                sendTelegramMessage("Error while updating data: {}.\nTender_Id = '{}'".format(str(e), SourceTender))
                continue
            TimeDict['DB'] += (datetime.now() - _Start).total_seconds()

            self._Processed += 1
        
        # Reindex table in TargetDB and close connections
        try:
            _Start = datetime.now()
            self._TargetCursor.execute("reindex table public.tenders")
            TimeDict['DB'] += (datetime.now() - _Start).total_seconds()

            self._SourceConnection.close()
            self._TagretConnection.close()
        except Exception as e:
            sendTelegramMessage("Error occured while closing connections inside of ProcessTenders function: {}".format(str(e)))


# MAIN 
sendTelegramMessage("Started our noble duty at {}".format(TimeDict['Start'].strftime('%d.%m.%Y %H:%M:%S')))

# Getting EDRPOUs
EDRPOUs = getCustomerEDRPOUs()

# Iterating EDRPOUs
for EDRPOU in EDRPOUs:
    # Getting Tenders for the EDRPOU
    SourceTenders = getTendersFromSource(EDRPOU)
    # Processing Tenders for the EDRPOU
    targetTender = TargetTender(SourceTenders, EDRPOU)

timeTaken = timeBeautifier((datetime.now() - TimeDict['Start']).total_seconds())
apiOps= timeBeautifier(TimeDict['API'])
dbOps = timeBeautifier(TimeDict['DB'])
sendTelegramMessage("Process has ended.\nTime taken = {}.\nAPI operations have taken = {}.\nDB operations have taken = {}.\nInserted {} tenders.".format(timeTaken, apiOps, dbOps, TimeDict['Processed']))

