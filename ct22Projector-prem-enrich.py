import pandas as pd
from pandas import read_csv
import json
from elasticsearch import Elasticsearch, helpers
# import sys
import os
# import pdb
import glob
from datetime import date
import metadata
from elasticsearch.exceptions import RequestError

class EnrichIngestion :

    def __init__(self, param_jason):
       print ("init")
       # self.es = Elasticsearch()
       with open(param_jason) as f:
          self.param_data = json.load(f)

       self.path = self.param_data["path"]
       self.index = self.param_data["indexToWriteIn"]
       self.pathlength = len(self.path)
       self.pathProcessed= self.param_data["pathProcessed"]
       self.esServer = self.param_data["ESServer"]
       self.esUser = self.param_data["ESUser"]
       self.esPwd = self.param_data["ESPwd"]
       # es = Elasticsearch(['http://localhost:9200'], http_auth=('user', 'pass'))
       self.es = Elasticsearch([self.esServer], http_auth=(self.esUser, self.esPwd))
       print ("After connection to ES")
       self.sqlite = self.param_data["sqlite"]
       self.na_nodes = {}
       self.na_dbusers = {}

       # --- Set up the Kanban - Semaphore for "Process in progress"
       self.InProg = self.path + 'FullSQL_Simple_Audit_In_Progress'
       if os.path.exists(self.InProg) == True :
           print ('Process in Progress - Exiting')
           exit(0)
       else:
           os.system('touch ' + self.InProg)


    def main_process(self):
        print("Start Basic Ingestion", "  Arg 1 : Index name (optional) ")
        # ---- Get the Metdata
        self.metadata()

        # ---- Get list of files
        DataFiles = self.DataFile_List()
        print('Nbr of Files to Process : ', len(DataFiles))
        if len(DataFiles) == 0:
            print("NO file to process ")
            os.system('rm -f ' + self.InProg)
            exit(0)

        # ---- Process ALL files
        self.process_all_files(DataFiles)

        # df = p1.read_file_tbi()

        # p1.ingest(df)

        print("End of  Basic Ingestion")

    # ---- getting Client, Servers and DBUsers MetaData
    def metadata(self):
            print("In MetaData")
            p1 = metadata.MetaData(self.sqlite)

            # print ("In MetaData")
            # ---- Get Nodes
            # print(myListIPs)
            self.myListNodes = p1.get_nodes()
            # breakpoint()
            columns_nodes = self.myListNodes.columns.tolist()
            for column in columns_nodes:
                self.na_nodes[column] = None
            # breakpoint()
            # print(type(self.myListIPs), " -- " ,self.myListIPs)

            # ---- Get DB Users
            self.myListDBUsers = p1.get_DBUsers()
            # print(type(self.myListIPs), " -- " ,self.myListIPs)
            columns_dbusers = self.myListDBUsers.columns.tolist()
            for column in columns_dbusers:
                self.na_dbusers[column] = None
            # breakpoint()

    # --- Getting the DAM Files to be Processed  ----
    def DataFile_List(self):
       DataFile=[]
       DataFiles=[]
       # csvFiles=glob.glob(self.path + "*" + self.collector + "*FSQL*.csv")
       csvFiles=glob.glob(self.path + "*FSQL*.csv")
       # breakpoint()
       for file in csvFiles:
           if "FSQL" in file:
              COLL = file.split('_')[1]
              DataFile.append(COLL)
              DataFile.append(file)
              DataFiles.append(DataFile)
              DataFile=[]
       return (DataFiles)

    # --- Process All Files
    def process_all_files(self,DataFiles):
    # --- Loop for each DAM data file
      for datafile in DataFiles:
        print('Will be Processing : ',datafile)
        os.system('printf "' + datafile[1] + '\n" >> ' + self.InProg )
        # self.SonarGSource =  datafile[0]

        # -- Process One File
        self.fullSQLMany=[]
        df=self.read_file_tbi(datafile)

        # -- Upload into ES (ETL)
        print("Nbr of Docs to be Inserted", len(df))
        # print (" list SQLCounters ALL FILES " , self.p1.listSqlCounters)
        if len(df) > 0 :
           # --- insert enrich full sql
           self.ingest(df, datafile)

        os.system('rm -f ' + self.InProg)

        self.rename_file(datafile)

      return()

    # --- Move FullSQL csv file to Processed Folder
    def rename_file(self,datafile):

      shortname=datafile[1][self.pathlength:]
      print ("Rename as processed" , shortname)
      os.rename(datafile[1],self.pathProcessed + shortname)

    def read_file_tbi(self,datafile) :

       df = read_csv(datafile[1])
       df = df.fillna("N/A")

       # Parse dates column
       # df['Timestamp'] = pd.to_datetime(df['Timestamp'])
       # df['Session Start'] = pd.to_datetime(df['Session Start'])
       columns = list(df.columns)
       columns[0] = "UTC Offset"
       df.columns = columns
       # print ( "DF : ", df.info())
       enrich_dict=[]
       for i in range(df.shape[0]):
           line = df.iloc[[i]]
           # print(line)
           # breakpoint()
           enrich_line = self.process_one_line(line)
           enrich_dict.append(enrich_line)

       print(enrich_dict)
       return(enrich_dict)

    def process_one_line (self,line) :
       # print (line)
       # breakpoint()
       # print(line['Timestamp'])
       line_dict = line.to_dict(orient='records')[0]
       # breakpoint()
       line_dict["Server Metadata"] = self.lookup_Server(line['Server Host Name'].item())
       line_dict["Client Metadata"] = self.lookup_Client(line['Client Host Name'].item())
       line_dict["DBUser Metadata"] = self.lookup_DBUser(line['DB User Name'].item())

       return(line_dict)

    # ---- Lookups into the Metadata  -----
    def lookup_Server(self,Server):
      # print ("lookup_Server   ", type(Server), "   ", Server)
      # breakpoint()
      server_metadata = self.myListNodes[self.myListNodes['Hostname'] == Server]
      # breakpoint()
      if server_metadata.empty == True:
          return (self.na_nodes)
      else:
          server_metadata = server_metadata.to_dict(orient='records')[0]
          return (server_metadata)

    # ---- Lookups into the Metadata  -----
    def lookup_Client(self,Client):
      # print ("lookup_Client")
      client_metadata = self.myListNodes[self.myListNodes['Hostname'] == Client]
      # breakpoint()
      if client_metadata.empty == True:
          return (self.na_nodes)
      else:
          client_metadata = client_metadata.to_dict(orient='records')[0]
          return (client_metadata)

      # breakpoint()

    # ---- Lookups into the Metadata  -----
    def lookup_DBUser(self,DBUser):
      # print ("lookup_DBUser")
      # breakpoint()
      DBUser_metadata = self.myListDBUsers[self.myListDBUsers['DB User Name'] == DBUser]
      # breakpoint()
      if DBUser_metadata.empty == True:
          return (self.na_dbusers)
      else:
          DBUser_metadata = DBUser_metadata.to_dict(orient='records')[0]
          return (DBUser_metadata)

      # breakpoint()

    def ingest(self, df, datafile):
      print ("In ingest")
      # df_json=df.to_json(orient='records', date_format = 'iso')
      # df_json=df.to_json()
      # parsed=json.loads(df)
      # df = [{'name' : 'aaaa'}]
      # df = [{'UTC Offset': -5, 'Access Rule Description': 'Policy Rule 1', 'Full Sql': 'Select * from SSN', 'Instance ID': 5.84817e+17, 'Records Affected': 2000000, 'Response Time': 20000, 'Session Id': 5.84817e+17, 'Succeeded': 1, 'Timestamp': '2025-01-01T14:30:00+0000', 'Returned Data': 'nan', 'Analyzed Client IP': '172.22.87.40', 'Client Host Name': 'LAPTOP_1', 'DB User Name': 'DBUSER_1', 'Database Name': 'DB_1', 'Network Protocol': 'TCP', 'OS User': 'OSU_1', 'Server Host Name': 'SERVER_1', 'Server IP': '1.1.1.1', 'Server Port': 1433, 'Server Type': 'MS SQL SERVER', 'Service Name': 'MS SQL SERVER', 'Client Port': 51845, 'Source Program': 'MICROSOFT SQL SERVER MANAGEMENT STUDIO', 'Session Start': '2021-04-12T14:30:00+0000', 'Objects and Verbs': 'SELECT SSN', 'Uid Chain': 'nan', 'Uid Chain Compressed': 'nan', 'Original SQL': 'Select * from SSN', 'App User Name': 'nan', 'Server Metadata': {'Hostname': 'SERVER_1', 'IP': '1.1.1.1', 'Env': 'PROD', 'Location': 'Internal', 'Physical Type': 'Node', 'Node Type': 'Database', 'PII': 1, 'Org': 'Neuro', 'Business Owner': 'Rahman MD', 'IT Owner': 'Grouillot'}, 'Client Metadata': {'Hostname': 'LAPTOP_1', 'IP': None, 'Env': 'NON PROD', 'Location': 'External', 'Physical Type': 'Laptop', 'Node Type': 'Windows', 'PII': 0, 'Org': 'Neuro', 'Business Owner': 'Rahman MD', 'IT Owner': 'Grouillot'}, 'DBUser Metadata': {'DB User Name': 'DBUSER_1', 'Acct Type': 'Personal', 'email': 'G@G.net', 'Shared': 0, 'Contact': 'Grouillot'}}]
      # df=[{'UTC Offset': -5, 'Access Rule Description': 'Policy Rule 1', 'Full Sql': 'INSERT into SSN_ROGUE from (Select * from SSN)', 'Instance ID': 5.84817e+17, 'Records Affected': 2229022, 'Response Time': 400000, 'Session Id': 5.84817e+17, 'Succeeded': 1, 'Timestamp': '2025-01-16T14:30:00Z', 'Returned Data': 'N/A', 'Analyzed Client IP': '172.22.87.40', 'Client Host Name': 'SERVER_APP_B', 'DB User Name': 'DBUSER_3', 'Database Name': 'DB_8', 'Network Protocol': 'TCP', 'OS User': 'OSU_1', 'Server Host Name': 'SERVER_51', 'Server IP': '1.1.1.1', 'Server Port': 1433, 'Server Type': 'ORACLE', 'Service Name': 'ORACLE', 'Client Port': 51845, 'Source Program': 'Toad', 'Session Start': '2021-04-12T14:30:00Z', 'Objects and Verbs': 'INSERT SSN', 'Uid Chain': 'N/A', 'Uid Chain Compressed': 'N/A', 'Original SQL': 'INSERT into SSN_ROGUE from (Select * from SSN)', 'App User Name': 'N/A', 'Server Metadata': {'Hostname': 'SERVER_51', 'IP': '1.1.1.11', 'Env': 'QA', 'Location': 'Internal', 'Physical Type': 'Node', 'Node Type': 'Database', 'PII': 1, 'Org': 'Surgery', 'Business Owner': 'Mr Petit', 'IT Owner': 'De Base'}, 'Client Metadata': {'Hostname': 'N/A', 'IP': 'N/A', 'Env': 'N/A', 'Location': 'N/A', 'Physical Type': 'N/A', 'Node Type': 'N/A', 'PII': 'N/A', 'Org': 'N/A', 'Business Owner': 'N/A', 'IT Owner': 'N/A'}, 'DBUser Metadata': {'DB User Name': 'N/A', 'Acct Type': 'N/A', 'email': 'N/A', 'Shared': 'N/A', 'Contact': 'N/A'}}]
      # df = [{'UTC Offset': -5, 'Access Rule Description': 'Policy Rule 1', 'Full Sql': 'INSERT into SSN_ROGUE from (Select * from SSN)','Session Start': '2021-04-12T14:30:00Z'}]
      # df = [{'Instance ID': 5.84817e+17, 'Records Affected': 2229022, 'Response Time': 400000, 'Session Start': '2021-04-12T14:30:00Z'}]
      # df = [{'Session Id': 5.84817e+17, 'Succeeded': 1, 'Timestamp': '2025-01-16T14:30:00Z', 'Returned Data': 'N/A', 'Analyzed Client IP': '172.22.87.40', 'Client Host Name': 'SERVER_APP_B', 'DB User Name': 'DBUSER_3', 'Database Name': 'DB_8', 'Network Protocol': 'TCP', 'OS User': 'OSU_1', 'Server Host Name': 'SERVER_51', 'Server IP': '1.1.1.1', 'Server Port': 1433, 'Server Type': 'ORACLE','Session Start': '2021-04-12T14:30:00Z'}]
      # df = [{'Client Port': 51845, 'Source Program': 'Toad', 'Session Start': '2021-04-12T14:30:00Z', 'Objects and Verbs': 'INSERT SSN', 'Uid Chain': 'N/A', 'Uid Chain Compressed': 'N/A', 'Original SQL': 'INSERT into SSN_ROGUE from (Select * from SSN)', 'App User Name': 'N/A'}]
      # df =[{'Server Metadata': {'Hostname': 'SERVER_51', 'IP': '1.1.1.11', 'Env': 'QA', 'Location': 'Internal', 'Physical Type': 'Node', 'Node Type': 'Database', 'PII': 1, 'Org': 'Surgery', 'Business Owner': 'Mr Petit', 'IT Owner': 'De Base'}, 'Session Start': '2021-04-12T14:30:00Z'}]
      # df = [{'Session Start': '2021-04-12T14:30:00Z','Client Metadata': {'Hostname': 'N/A', 'IP': 'N/A', 'Env': 'N/A', 'Location': 'N/A', 'Physical Type': 'N/A', 'Node Type': 'N/A', 'PII': 'N/A', 'Org': 'N/A', 'Business Owner': 'N/A', 'IT Owner': 'N/A'}}]
      # df = [{'Session Start': '2021-04-12T14:30:00Z','Client Metadata': {'Hostname': 'N/A' , 'IP': 'N/A', 'Env': 'N/A', 'Location': 'N/A', 'Physical Type': 'N/A', 'Node Type': 'N/A' , 'PII': 'N/A'}}]
      file = datafile[1]
      fileSplit_1 = file.split('/')
      print(fileSplit_1)
      fileName = fileSplit_1[len(fileSplit_1) - 1]
      print(fileName)
      fileSplit_2 = fileName.split('_')
      print(fileSplit_2)
      COLL = fileSplit_2[1]
      print(COLL)
      fileDate = fileSplit_2[5][:8]
      try:
         # response = helpers.bulk(self.es,parsed, index=sys.argv[2])
         response = helpers.bulk(self.es,df, index=self.index+"enrich-"+str(fileDate[:4])+"."+str(fileDate[4:6])+"."+str(fileDate[6:8]))
         # print ("ES response : ", response )
      # except Exception as e:
      except RequestError as e:
         print ("ES Error :", e)
         error_details = e.error
         print(f"Error Type: {error_details['type']}")
         print(f"Error Reason: {error_details['reason']}")
         if 'root_cause' in error_details:
               print(f"Root Cause: {error_details['root_cause'][0]['reason']}")
         #Print the whole error detail for further inspection
         print(f"Full error details: {error_details}")

      return(len(df))
