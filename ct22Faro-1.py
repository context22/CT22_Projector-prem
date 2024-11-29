import pandas as pd
from pandas import read_csv
import json
from elasticsearch import Elasticsearch, helpers
import sys
import os
import pdb
import glob
from datetime import date


class BasicIngestion :

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

       # --- Set up the Kanban - Semaphore for "Process in progress"
       self.InProg = self.path + 'FullSQL_Simple_Audit_In_Progress'
       if os.path.exists(self.InProg) == True :
           print ('Process in Progress - Exiting')
           exit(0)
       else:
           os.system('touch ' + self.InProg)


    # --- Getting the DAM Files to be Processed  ----
    def DataFile_List(self):
       DataFile=[]
       DataFiles=[]
       # csvFiles=glob.glob(self.path + "*" + self.collector + "*FSQL*.csv")
       csvFiles=glob.glob(self.path + "*FSQL*.csv")
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
           self.ingest(df)

        os.system('rm -f ' + self.InProg)

        self.rename_file(datafile)

      return()


    def read_file_tbi(self,datafile) :

       df = read_csv(datafile[1])

       # Parse dates column
       df['Timestamp'] = pd.to_datetime(df['Timestamp'])
       df['Session Start'] = pd.to_datetime(df['Session Start'])
       columns = list(df.columns)
       columns[0] = "UTC Offset"
       df.columns = columns
       # print ( "DF : ", df.info())

       # exit(0)

       return df


    def ingest(self, df):
      print ("In ingest")
      df_json=df.to_json(orient='records', date_format = 'iso')
      # df_json=df.to_json()
      parsed=json.loads(df_json)
      today=date.today()
      year = today.year
      month = today.month
      day = today.day
      try:
         # response = helpers.bulk(self.es,parsed, index=sys.argv[2])
         response = helpers.bulk(self.es,parsed, index=self.index+str(year)+str(month)+str(day))
         # print ("ES response : ", response )
      except Exception as e:
         print ("ES Error :", e)

      return(len(parsed))

    # --- Move FullSQL csv file to Processed Folder
    def rename_file(self,datafile):

      shortname=datafile[1][self.pathlength:]
      print ("Rename as processed" , shortname)
      os.rename(datafile[1],self.pathProcessed + shortname)


# --- Main  ---
if __name__ == '__main__':
    # printdatafile("Start Basic Ingestion","  Arg 1 : filename , Arg 2 : Index name")
    print("Start Basic Ingestion","  Arg 1 : Index name (optional) ")

    p1 = BasicIngestion("param_data.json")

    # ---- Get list of files
    DataFiles = p1.DataFile_List()
    print ('Nbr of Files to Process : ' , len(DataFiles))
    if len(DataFiles) == 0 :
       print ("NO file to process " )
       os.system('rm -f ' + p1.InProg)
       exit(0)

    # ---- Process ALL  file
    p1.process_all_files(DataFiles)



    # df = p1.read_file_tbi()

    # p1.ingest(df)

    print("End of  Basic Ingestion")
