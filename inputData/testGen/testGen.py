import json
from datetime import datetime
import time
import os
import sys
import glob
# from elasticsearch import Elasticsearch, helpers
import pandas as pd
import pdb
from icecream import ic
# # # import metadata
from datetime import date
import numpy as np


class testgen:

    def __init__(self, param_jason):
       print ("init")
       # self.es = Elasticsearch()
       with open(param_jason) as f:
          self.param_data = json.load(f)

       self.df_conv = pd.read_csv('convdata.csv')
       print (self.df_conv)
       self.path = self.param_data["path"]
       self.pathlength = len(self.path)
       self.pathProcessed= self.param_data["pathProcessed"]
       # self.index = self.param_data["index"]
       # self.esServer = self.param_data["ESServer"]
       # self.esUser = self.param_data["ESUser"]
       # self.esPwd = self.param_data["ESPwd"]
       # self.sqlite = self.param_data["sqlite"]
       self.fullSQLMany=[]


       self.InProg = self.path + 'FSQL_Convert_In_Progress'
       # breakpoint()
       if os.path.exists(self.InProg) == True :
          print ("FSQL_Convert_In_Progress - Exiting" )
          exit(0)
       else:
          os.system('touch ' + self.InProg)

       self.myListCollectors = []
       self.csvFiles=[]
       self.DataFiles=[]


    def DataFile_List(self):
        print ("DataFile_List")
        self.csvFiles=glob.glob(self.path + "*EXP_FSQL_NODEP*.csv")
        if len(self.csvFiles) == 0:
           print ("No File to Process")
           os.system('rm -f ' + self.path + 'FSQL_Convert_In_Progress')
           sys.exit()
        DataFile=[]
        for file in self.csvFiles:
          if "FSQL" in file:
           fileSplit_1 = file.split('/')
           print (fileSplit_1)
           fileName= fileSplit_1[len(fileSplit_1)-1]
           print (fileName)
           fileSplit_2 = fileName.split('_')
           print (fileSplit_2)
           COLL = fileSplit_2[1]
           print (COLL)
           oldDate = fileSplit_2[5][:8]
           oldPart = fileSplit_2[5][8:]
           print (oldDate)
           newDate = input ("New date ? YYYYMMDD ")
           print (newDate)
           print (oldPart)
           print ("Collector : " , COLL)
           COLL_new = input ("Collector new Name ? ")
           print ("New Collector : " , COLL_new)
           newFile = fileSplit_2[0] + "_" + COLL_new  + "_" + fileSplit_2[2] + "_" + fileSplit_2[3] + "_" + fileSplit_2[4] + "_" + newDate + oldPart
           print (newFile)

           DataFile.append(COLL)
           DataFile.append(file)
           DataFile.append(newDate)
           DataFile.append(COLL_new)
           DataFile.append(newFile)
           self.DataFiles.append(DataFile)
           DataFile=[]
           print (self.DataFiles)

    def processOneFile(self,datafile):

      print ("processOneFile " , datafile)
      coll = datafile[0]
      df = pd.read_csv(datafile[1])
      self.field_list=df.columns
      print (self.field_list)

      # print (self.df_conv)
      field_list_conv=self.df_conv.columns
      print (field_list_conv[0])
      # breakpoint()

      # for i in range(df.shape[0]):
      #      line = df.iloc[[i]]
      #       print (type(line), " -- " , line )
      #       self.process_one_line(line)

      # df = df.sort_values(by='S-TAP Host').reset_index(drop=True)
      # self.df_conv = self.df_conv.sort_values(by='Old S-TAP Host').reset_index(drop=True)
      df['Timestamp'] =self.df_conv['Timestamp']
      df['Access Rule Description'] =self.df_conv['Access Rule Description']
      df['Full Sql'] =self.df_conv['Full Sql']
      df['Records Affected'] =self.df_conv['Records Affected']
      df['Client Host Name'] =self.df_conv['Client Host Name']
      df['DB User Name'] =self.df_conv['DB User Name']
      df['Database Name'] =self.df_conv['Database Name']
      df['Server Host Name'] =self.df_conv['Server Host Name']
      df['Server Type'] =self.df_conv['Server Type']
      df['Service Name'] =self.df_conv['Service Name']
      df['Source Program'] =self.df_conv['Source Program']
      df['Objects and Verbs'] =self.df_conv['Objects and Verbs']
      df['Original SQL'] =self.df_conv['Original SQL']


      print (df.head())
      # Write the DataFrame to a CSV file
      df.to_csv('data.csv', index=False)

      return(df)

    def mainProcess(self):    
        print("Start FSQL Convert Enrichment")

        # p1 = stapsdown("param_data.json")

        # p1.convdata()

        self.DataFile_List()
        # --- Loop for each DAM data file
        doc_count_total = 0
        for datafile in self.DataFiles:
               print('Processing : ',datafile)
               #   Process ONE file  -------------
               df=self.processOneFile(datafile)
               # move the processed file to Processed directory
               print ("datafile for current file " , datafile[4])
               print (self.pathProcessed + datafile[4])
               # os.rename(datafile[1],self.pathProcessed + datafile[4])
               newPath = self.pathProcessed + datafile[4]
               # df.to_csv('data.csv', index=False)
               df.to_csv(newPath, index=False)
               # os.rename(datafile[1],self.pathProcessed + shortname)
               # doc_count_total = doc_count_total + doc_count
               # print ('Nbr of Docs Processed' , doc_count_total)
               os.system('rm -f ' + self.path + 'FSQL_Convert_In_Progress')
        print("End FSQL Convertion")
        return(0)
