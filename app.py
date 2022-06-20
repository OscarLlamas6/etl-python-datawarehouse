import mysql.connector
from dotenv import load_dotenv
import traceback
import pandas
import os
import pyodbc 
from datetime import datetime

from createTables import *
from fillTables import *

# Setting env variables
load_dotenv()

# Mysql env var
MYSQL_DB_HOST = os.environ['MYSQL_DB_HOST']
MYSQL_DB_USER = os.environ['MYSQL_DB_USER']
MYSQL_DB_PASS = os.environ['MYSQL_DB_PASS']
MYSQL_DB_NAME = os.environ['MYSQL_DB_NAME']

# SQL Server en var
SQLSV_DB_HOST = os.environ['SQLSV_DB_HOST']
SQLSV_DB_USER = os.environ['SQLSV_DB_USER']
SQLSV_DB_PASS = os.environ['SQLSV_DB_PASS']
SQLSV_DB_NAME = os.environ['SQLSV_DB_NAME']

# DBs conn
myDB = None
sqlServerDB = None
driverSettings = None

try:
    # Setting MySQL conn
    myDB = mysql.connector.connect(
    host=MYSQL_DB_HOST,
    user=MYSQL_DB_USER,
    password=MYSQL_DB_PASS,
    database=MYSQL_DB_NAME,
    auth_plugin='mysql_native_password') 
    
    # Setting MSSQVSV conn  
    sqlServerDB = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SQLSV_DB_HOST+';DATABASE='+SQLSV_DB_NAME+';UID='+SQLSV_DB_USER+';PWD='+ SQLSV_DB_PASS, autocommit=True)
    cursorSqlServer = sqlServerDB.cursor()
    cursorSqlServer.execute('USE [master]')
    cursorSqlServer.execute('''IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'PROYECTO1')
                                BEGIN
                                    CREATE DATABASE [PROYECTO1]
                                END''')
    cursorSqlServer.execute('USE [PROYECTO1]')
    cursorSqlServer.execute('''IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'PROYECTO1')) 
                                BEGIN
                                    EXEC ('CREATE SCHEMA PROYECTO1')
                                END''')
    cursorSqlServer.close()
    
    # Setting logs file
    myFile = open("logs.txt", "w")
    print("------------ LOGS PROYECTO 1 FASE 1 | SEMI2-GRUPO1 ------------", file=myFile)
    print(file=myFile)
    myFile.close()
       
except Exception:
    traceback.print_exc()
    input("\x1b[1;31m"+"Presiona ENTER para continuar...")

class CLI():
    
    def __init__(self):    
        while(True):
            os.system('cls||clear')
            print("\x1b[1;31m"+"------------------ SEMINARIO DE SISTEMAS 2: PROYECTO 1 -----------------")
            print("\x1b[1;36m"+"-------------------------------- GRUPO 1 --------------------------------")
            menu()        
            keyInput = input("\x1b[1;37m"+"")    
            if keyInput == "1":
                extractData()
            if keyInput == "5" or keyInput.lower() == "exit":
                print("\x1b[1;31m"+"\nHASTA LA PROXIMA :D")
                exit()

def selectQuery():
    while(True):
        os.system('cls||clear')
        
        print("\x1b[1;31m"+"------------------ SEMINARIO DE SISTEMAS 2: PROYECTO 1 -----------------")
        print("\x1b[1;36m"+"-------------------------------- GRUPO 1 --------------------------------")
        queriesMenu()        
        keyInput = input("\x1b[1;37m"+"")    
        if keyInput == "1":
            print("hola")
        if keyInput == "11" or keyInput.lower() == "exit":
            break            
                
def menu():
    print("\x1b[1;34m"+"\n---------------------------- ELIGE UNA OPCION ----------------------------")
    print("\x1b[1;32m"+"1) INICIAR ETL")
    print("\x1b[1;31m"+"2) CREAR MODELO")
    print("\x1b[1;36m"+"5) SALIR\n")
    print("\x1b[1;32m"+"USAC ", end='')
    print("\x1b[1;33m"+"> ", end='')
    
def queriesMenu():
    print("\x1b[1;34m"+"\n-------------------------- ELIGE UNA CONSULTA --------------------------")
    print("\x1b[1;35m"+"1) CONSULTA 1")
    print("\x1b[1;32m"+"2) CONSULTA 2") 
    print("\x1b[1;33m"+"3) CONSULTA 3")
    print("\x1b[1;31m"+"4) CONSULTA 4")
    print("\x1b[1;37m"+"5) CONSULTA 5")
    print("\x1b[1;35m"+"6) CONSULTA 6")
    print("\x1b[1;32m"+"7) CONSULTA 7")
    print("\x1b[1;33m"+"8) CONSULTA 8")
    print("\x1b[1;34m"+"9) CONSULTA 9")
    print("\x1b[1;31m"+"10) CONSULTA 10")
    print("\x1b[1;36m"+"11) SALIR\n")
    print("\x1b[1;32m"+"USAC ", end='')
    print("\x1b[1;33m"+"> ", end='')

def createModel():
    try:
        print("\x1b[1;33m"+'Modelo creado :D')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al crear modelo :o')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    
def dropModel():
    try:
        # Droping Schema
        print("\x1b[1;33m"+'Modelo eliminado :(')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al eliminar modelo :o')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")

def extractData():
    try:
        print("\x1b[1;33m"+"\n------------------------- ETL INFORMACION -------------------------")
        # Extrayendo info de pib.csv         
        myFile = open("logs.txt", "a")
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Extrayendo informacion pib.csv", file=myFile) 
        csvData = pandas.read_csv(r'data/pib.csv')
        df = pandas.DataFrame(csvData)
        df = df.fillna(value=0)

        cursor = myDB.cursor()
        cursorSqlServer = sqlServerDB.cursor()
        cursorSqlServer.execute('USE [PROYECTO1]')

        #Borrando tabla temporal para pib MYSQL
        print('BORRANDO TEMPORAL PIB MYSQL')
        cursor.execute(DROP_TEMPORAL_PIB, ())

        #Borrando tabla temporal para pib SQLSERVER
        print('BORRANDO TEMPORAL PIB SQLSERVER')
        cursorSqlServer.execute(DROP_TEMPORAL_PIB_MSSQL)

        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Creando tablatemporal temporalPIB", file=myFile)
        #Creando tabla temporal para pib MYSQL
        print('CREANDO TEMPORAL PIB MYSQL')
        cursor.execute(CREATE_TEMPORAL_PIB, ())

        #Creando tabla temporal para pib SQLSERVER
        print('CREANDO TEMPORAL PIB SQLSERVER')
        cursorSqlServer.execute(CREATE_TEMPORAL_PIB_MSSQL)
        
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Transformando informacion pib.csv", file=myFile)
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Cargando informacion pib.csv", file=myFile)
        # Iterando sobre cada registro del csv 
        for row in df.itertuples(index=False):        
            #print(row[0])

            if row[0] != 0 :

                cursor.execute(LLENADO_TEMPORAL_PIB,
                (
                row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19],
                row[20], row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28], row[29],
                row[30], row[31], row[32], row[33], row[34], row[35], row[36], row[37], row[38], row[39],
                row[40], row[41], row[42], row[43], row[44], row[45], row[46], row[47], row[48], row[49],
                row[50], row[51], row[52], row[53], row[54], row[55], row[56], row[57], row[58], row[59],
                row[60], row[61], row[62], row[63], row[64], row[65]
                ))

                cursorSqlServer.execute(LLENADO_TEMPORAL_PIB_SQLSERVER,
                (
                row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19],
                row[20], row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28], row[29],
                row[30], row[31], row[32], row[33], row[34], row[35], row[36], row[37], row[38], row[39],
                row[40], row[41], row[42], row[43], row[44], row[45], row[46], row[47], row[48], row[49],
                row[50], row[51], row[52], row[53], row[54], row[55], row[56], row[57], row[58], row[59],
                row[60], row[61], row[62], row[63], row[64], row[65]
                ))

        #Borrando tabla temporal de inflacion MYSQL
        print('BORRANDO TEMPORAL INFLACION MYSQL')
        cursor.execute(DROP_TEMPORAL_INFLACION, ())

        #Borrando tabla temporal de inflacion SQLSERVER
        print('BORRANDO TEMPORAL INFLACION SQLSERVER')
        cursorSqlServer.execute(DROP_TEMPORAL_INFLACION_MSSQL, ())

        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Creando tablatemporal temporalInflacion", file=myFile)
        #Creando tabla temporal para inflacion MYSQL
        print('CREANDO TEMPORAL INFLACION MYSQL')
        cursor.execute(CREATE_TEMPORAL_INFLACION, ())

        #Creando tabla temporal para inflacion SQLSERVER
        print('CREANDO TEMPORAL INFLACION SQLSERVER')
        cursorSqlServer.execute(CREATE_TEMPORAL_INFLACION_MSSQL, ())

        # Extrayendo info de inflacion.csv
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Extrayendo informacion inflacion.csv", file=myFile)
        csvData = pandas.read_csv(r'data/inflacion.csv')
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Transformando informacion inflacion.csv", file=myFile)
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Cargando informacion inflacion.csv", file=myFile)
        df = pandas.DataFrame(csvData)
        df = df.fillna(value=0)
        # Iterando sobre cada registro del csv 
        for row in df.itertuples(index=False):        
            #print(row[0])
            if row[0] != 0 :
                cursor.execute(LLENADO_TEMPORAL_INFLACION,
                (
                row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19],
                row[20], row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28], row[29],
                row[30], row[31], row[32], row[33], row[34], row[35], row[36], row[37], row[38], row[39],
                row[40], row[41], row[42], row[43], row[44], row[45], row[46], row[47], row[48], row[49],
                row[50], row[51], row[52], row[53], row[54], row[55], row[56], row[57], row[58], row[59],
                row[60], row[61], row[62], row[63], row[64], row[65]
                ))

                cursorSqlServer.execute(LLENADO_TEMPORAL_INFLACION_SQLSERVER,
                (
                row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19],
                row[20], row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28], row[29],
                row[30], row[31], row[32], row[33], row[34], row[35], row[36], row[37], row[38], row[39],
                row[40], row[41], row[42], row[43], row[44], row[45], row[46], row[47], row[48], row[49],
                row[50], row[51], row[52], row[53], row[54], row[55], row[56], row[57], row[58], row[59],
                row[60], row[61], row[62], row[63], row[64], row[65]
                ))
        
        myDB.commit()
        cursorSqlServer.commit()
        sqlServerDB.commit()
        cursor.close()
        myFile.close()
        print("\x1b[1;33m"+"ETL TERMINADO EXITOSAMENTE :D")
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al cargar informacion :(')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    
myApp = CLI()