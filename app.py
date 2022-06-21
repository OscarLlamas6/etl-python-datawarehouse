import mysql.connector
from dotenv import load_dotenv
import traceback
import pandas
import os
import pyodbc 
from datetime import datetime
from halo import Halo
import time

from createTables import *
from fillTables import *
from crearDataMarts import *
from cargarDatamarts import *

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
        try:
            while(True):
                os.system('cls||clear')
                print("\x1b[1;31m"+"------------------ SEMINARIO DE SISTEMAS 2: PROYECTO 1 -----------------")
                print("\x1b[1;36m"+"-------------------------------- GRUPO 1 --------------------------------")
                menu()        
                keyInput = input("\x1b[1;37m"+"")    
                if keyInput == "1":
                    extractData()
                if keyInput == "2":
                    createModel()
                if keyInput == "3":
                    loadData()
                if keyInput == "4":
                    selectQuery()
                if keyInput == "5":
                    dropDatamarts()
                    crearDatamarts()
                if keyInput == "6":
                    cargarDatamartsNuevos()
                if keyInput == "7" or keyInput.lower() == "exit":
                    print("\x1b[1;31m"+"\nHASTA LA PROXIMA :D")
                    break
        except Exception as Ex:
            print(Ex)
        
def selectQuery():
    while(True):
        os.system('cls||clear')
        
        print("\x1b[1;31m"+"------------------ SEMINARIO DE SISTEMAS 2: PROYECTO 1 -----------------")
        print("\x1b[1;36m"+"-------------------------------- GRUPO 1 --------------------------------")
        queriesMenu()        
        keyInput = input("\x1b[1;37m"+"")    
        if keyInput == "1":
            Query1()
        if keyInput == "2":
            Query2()
        if keyInput == "13" or keyInput.lower() == "exit":
            break            
                
def menu():
    print("\x1b[1;34m"+"\n---------------------------- ELIGE UNA OPCION ----------------------------")
    print("\x1b[1;32m"+"1) INICIAR ETL")
    print("\x1b[1;31m"+"2) CREAR MODELO")
    print("\x1b[1;33m"+"3) CARGAR INFORMACION")
    print("\x1b[1;33m"+"4) CONSULTAS")
    print("\x1b[1;35m"+"5) CREAR DATAMARTS")
    print("\x1b[1;35m"+"7) CARGAR DATAMARTS")
    print("\x1b[1;36m"+"7) SALIR\n")
    print("\x1b[1;32m"+"USAC ", end='')
    print("\x1b[1;33m"+"> ", end='')
    
def queriesMenu():
    print("\x1b[1;34m"+"\n-------------------------- ELIGE UNA CONSULTA --------------------------")
    print("\x1b[1;35m"+"1) TOP 10 CON MEJOR PIB EN 2021")
    print("\x1b[1;32m"+"2) TOP 10 CON MENOR PIB EN 2018") 
    print("\x1b[1;33m"+"3) CONSULTA 3")
    print("\x1b[1;31m"+"4) CONSULTA 4")
    print("\x1b[1;37m"+"5) CONSULTA 5")
    print("\x1b[1;35m"+"6) CONSULTA 6")
    print("\x1b[1;32m"+"7) CONSULTA 7")
    print("\x1b[1;33m"+"8) CONSULTA 8")
    print("\x1b[1;34m"+"9) CONSULTA 9")
    print("\x1b[1;31m"+"10) CONSULTA 10")
    print("\x1b[1;32m"+"11) CONSULTA 11")
    print("\x1b[1;35m"+"12) CONSULTA 12")
    print("\x1b[1;36m"+"13) SALIR\n")
    print("\x1b[1;32m"+"USAC ", end='')
    print("\x1b[1;33m"+"> ", end='')

def createModel():
    #spinner = Halo(text='Creando modelo...', spinner='dots')
    try:
        #spinner.start()
        myFile = open("logs.txt", "a")
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Creando modelo en MSSQL Server", file=myFile) 
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
        cursorSqlServer.execute('DROP TABLE if exists [PROYECTO1].indicadorpais')
        cursorSqlServer.execute('DROP TABLE if exists [PROYECTO1].indicador')
        cursorSqlServer.execute('DROP TABLE if exists [PROYECTO1].pais')
        cursorSqlServer.execute('DROP TABLE if exists [PROYECTO1].fecha')
        cursorSqlServer.execute('''CREATE TABLE  [PROYECTO1].indicador(
                                    id_indicador int not null identity(1,1),
                                    indicador varchar(300),
                                    codigo_indicador varchar(300),
                                    constraint pk_indicador primary key(id_indicador)
                                )''')
        cursorSqlServer.execute('''CREATE TABLE [PROYECTO1].pais(
                                    id_pais int not null identity(1,1),
                                    pais varchar(300),
                                    codigo_pais varchar(300),
                                    constraint pk_pais primary key (id_pais)
                                )''')
        cursorSqlServer.execute('''CREATE TABLE [PROYECTO1].fecha(
                                    id_fecha int not null identity(1,1),
                                    year_field int not null,
                                    constraint pk_fecha primary key (id_fecha)
                                )''')
        cursorSqlServer.execute('''CREATE TABLE [PROYECTO1].indicadorpais(
                                    id_indicadorPais int not null identity(1,1),
                                    id_pais int,
                                    id_indicador int,
                                    id_fecha int,
                                    valor float,
                                    constraint pk_indicadorpais primary key (id_indicadorPais),
                                    constraint fk_indicadorpais_pais foreign key (id_pais) references [PROYECTO1].pais(id_pais),
                                    constraint fk_indicadorpais_indicador foreign key (id_indicador) references [PROYECTO1].indicador(id_indicador),
                                    constraint fk_indicadorpais_fecha foreign key (id_fecha) references [PROYECTO1].fecha(id_fecha)
                                )''')
        cursorSqlServer.close()
        myFile.close()
        #time.sleep(3)
        #spinner.succeed()
        #spinner.stop_and_persist(symbol='👽'.encode('utf-8'), text="Modelo creado correctamente :D")
        print("\x1b[1;33m"+'Modelo creado :D')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e:
        print(e)
        #spinner.stop()
        print('Error al crear modelo :o')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")

def crearDatamarts():
    try:
        cursorSqlServer = sqlServerDB.cursor()
        myFile = open("logs.txt", "a")
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Creando Datamarts...", file=myFile) 
        # Ahora se corren todos los scripts para crear el datamart de 
        # inflacion, impacto_mundia y combinado
        # estos se encuentran cada uno en un arreglo de strings definido en el archivo
        # createTAbles.py y se ejecutaran justo en el orden en que fueron agregados
        # en el arreglo mencionado - Sera de igual forma para todos los demas datamarts

        cursorSqlServer.execute('USE [PROYECTO1]')
        
        for query in SCRIPTS_DATAMART_INFLACION:
            cursorSqlServer.execute(query)

        print(date_time + " - Datamart Inflacion creado exitosamente...", file=myFile) 
                
        for query in SCRIPTS_DATAMART_IMPACTO:
            cursorSqlServer.execute(query)

        print(date_time + " - Datamart Impacto_Mundial creado exitosamente...", file=myFile) 
        
        for query in SCRIPTS_DATAMART_COMBINADO:
            cursorSqlServer.execute(query)

        print(date_time + " - Datamart Combinado creado exitosamente...", file=myFile)
        print(date_time + " - Datamarts creados con exito!", file=myFile) 

        cursorSqlServer.close()
        myFile.close()
        print("\x1b[1;33m"+'Datamarts creados con exito :D')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
        
    except Exception as e:
        print(e)
        #spinner.stop()
        print('Error al crear datamarts :o')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
  
def cargarDatamartsNuevos():
    try:
        cursorSqlServer = sqlServerDB.cursor()
        myFile = open("logs.txt", "a")
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Cargando datos en Datamarts...", file=myFile)

        for query in SCRIPTS_CARGA_DM_INFLACION:
            cursorSqlServer.execute(query)
        
        print(date_time + " - Datos de Datamart: Inflacion, cargados con exito", file=myFile)

        for query in SCRIPTS_CARGA_DM_IMPACTO:
            cursorSqlServer.execute(query)

        print(date_time + " - Datos de Datamart: Impacto_Mundial, cargados con exito", file=myFile)
        
        for query in SCRIPTS_CARGA_DM_COMBINADO:
            cursorSqlServer.execute(query)

        print(date_time + " - Datos de Datamart: Combinado, cargados con exito", file=myFile)
        print(date_time + " - Datamarts cargados con exito!", file=myFile)

        myFile.close()
        cursorSqlServer.close()
        print("\x1b[1;33m"+'Datamarts cargados con exito :D')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e:
        print(e)
        #spinner.stop()
        print('Error al crear datamarts :o')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")

def loadData():
    try:
        print("\x1b[1;34m"+"\n------------------------- CARGANDO INFORMACION -------------------------")
        myFile = open("logs.txt", "a")
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Cargando información a modelo en MSSQL Server", file=myFile) 
        cursorSqlServer = sqlServerDB.cursor()
        cursorSqlServer.execute('USE [master]')
        cursorSqlServer.execute('USE [PROYECTO1]')
        cursorSqlServer.execute('''INSERT INTO [PROYECTO1].pais (pais, codigo_pais)
                                SELECT COUNTRIES.countryName, COUNTRIES.countryCode FROM (
                                    SELECT countryName, countryCode FROM [PROYECTO1].temporalInflacion
                                    UNION ALL
                                    SELECT countryName, countryCode FROM [PROYECTO1].temporalPib
                                ) AS COUNTRIES
                                GROUP BY COUNTRIES.countryName, COUNTRIES.countryCode''')
        cursorSqlServer.execute('''INSERT INTO [PROYECTO1].indicador (indicador, codigo_indicador)
                                SELECT INDICATORS.indicatorName, INDICATORS.indicatorCode FROM (
                                    SELECT indicatorName, indicatorCode FROM [PROYECTO1].temporalInflacion
                                    UNION ALL
                                    SELECT indicatorName, indicatorCode FROM [PROYECTO1].temporalPib
                                ) AS INDICATORS
                                GROUP BY INDICATORS.indicatorName, INDICATORS.indicatorCode''')
        cursorSqlServer.execute('''INSERT INTO [PROYECTO1].fecha (year_field)
                                SELECT YEARS.YEAR_FIELD FROM  (
                                    SELECT  RIGHT(COLUMN_NAME, LEN(COLUMN_NAME) - 1) AS YEAR_FIELD
                                    FROM INFORMATION_SCHEMA.COLUMNS
                                    WHERE TABLE_NAME = 'temporalInflacion' AND COLUMN_NAME LIKE 'A%'
                                    UNION  ALL
                                    SELECT  RIGHT(COLUMN_NAME, LEN(COLUMN_NAME) - 1) AS COLUMN_NAME
                                    FROM INFORMATION_SCHEMA.COLUMNS
                                    WHERE TABLE_NAME = 'temporalPib' AND COLUMN_NAME LIKE 'A%'
                                ) AS YEARS
                                GROUP BY YEARS.YEAR_FIELD''')
        for x in range(1960, 2022):
            cursorSqlServer.execute('''INSERT INTO [PROYECTO1].indicadorpais(id_pais, id_indicador, id_fecha, valor)
                                select
                                (
                                SELECT top 1 id_pais FROM [PROYECTO1].pais as pa
                                INNER JOIN [PROYECTO1].temporalPib
                                ON pa.pais = tempPib.countryName
                                ),
                                (
                                SELECT top 1 id_indicador FROM [PROYECTO1].indicador as indi
                                INNER JOIN [PROYECTO1].temporalPib
                                ON indi.indicador = tempPib.indicatorName
                                ),
                                (
                                SELECT top 1 id_fecha FROM [PROYECTO1].fecha as fe
                                INNER JOIN [PROYECTO1].temporalPib
                                ON fe.year_field like '{x}%'
                                ),
                                (
                                tempPib.A{x}
                                )
                                from [PROYECTO1].temporalPib as tempPib'''.format(x=x))

            cursorSqlServer.execute('''INSERT INTO [PROYECTO1].indicadorpais(id_pais, id_indicador, id_fecha, valor)
                                select
                                (
                                SELECT top 1 id_pais FROM [PROYECTO1].pais as pa
                                INNER JOIN [PROYECTO1].temporalInflacion
                                ON pa.pais = tempInf.countryName
                                ),
                                (
                                SELECT top 1 id_indicador FROM [PROYECTO1].indicador as indi
                                INNER JOIN [PROYECTO1].temporalInflacion
                                ON indi.indicador = tempInf.indicatorName
                                ),
                                (
                                SELECT top 1 id_fecha FROM [PROYECTO1].fecha as fe
                                INNER JOIN [PROYECTO1].temporalInflacion
                                ON fe.year_field like '{x}%'
                                ),
                                (
                                tempInf.A{x}
                                )
                                from [PROYECTO1].temporalInflacion as tempInf'''.format(x=x))

        cursorSqlServer.close()
        myFile.close()
        print("\x1b[1;33m"+"SE HAN CARGADO LOS DATOS EXITOSAMENTE :D")
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al cargar informacion :(')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")

def dropDatamarts():
    try:
        cursorSqlServer = sqlServerDB.cursor()
        cursorSqlServer.execute('USE [PROYECTO1]')

        # Ahora se corren todos los scripts para borar el datamart  
        # y sus tablas inflacion, impacto_mundia y combinado
        # estos se encuentran cada uno en un arreglo de strings definido en el archivo
        # createTAbles.py y se ejecutaran justo en el orden en que fueron agregados
        # en el arreglo mencionado - Sera de igual forma para todos los demas datamarts
        for query in SCRIPTS_DROP_DATAMART_COMBINADO:
            cursorSqlServer.execute(query)

        for query in SCRIPTS_DROP_DATAMART_IMPACTO:
            cursorSqlServer.execute(query)

        for query in SCRIPTS_DROP_DATAMART_INFLACION:
            cursorSqlServer.execute(query)

        cursorSqlServer.close()
        print("\x1b[1;33m"+'Modelo eliminado :(')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al cargar informacion :(')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...") 

def dropModel():
    try:
        # Droping Schema
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
        cursorSqlServer.execute('DROP TABLE if exists [PROYECTO1].indicadorpais')
        cursorSqlServer.execute('DROP TABLE if exists [PROYECTO1].indicador')
        cursorSqlServer.execute('DROP TABLE if exists [PROYECTO1].pais')
        cursorSqlServer.execute('DROP TABLE if exists [PROYECTO1].fecha')
        cursorSqlServer.close()
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

def Query1():
    try:
        myFile = open("logs.txt", "a")
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Generando reporte de consulta #1", file=myFile) 
        cursorSqlServer = sqlServerDB.cursor()
        cursorSqlServer.execute('''SELECT T.year_field AS 'year', P.pais, T.valor FROM (
                    SELECT F.year_field, R.id_pais, R.valor FROM ( 
                    SELECT IP.id_pais, IP.valor, IP.id_fecha
                        FROM [PROYECTO1].indicadorpais AS IP
                        INNER JOIN [PROYECTO1].indicador AS I
                        ON IP.id_indicador = I.id_indicador
                        WHERE I.indicador LIKE 'Crecimiento del PIB%') AS R
                    INNER JOIN [PROYECTO1].fecha AS F ON R.id_fecha = F.id_fecha
                    WHERE F.year_field = 2020
                    ORDER BY R.valor DESC
                    OFFSET 0 ROWS
                    FETCH NEXT 10 ROWS ONLY) AS T
                    INNER JOIN [PROYECTO1].pais AS P
                    ON T.id_pais = P.id_pais''')
        myQueryFile = open("consulta1.txt", "w")
        print("------------ CONSULTA 1 ------------", file=myQueryFile)
        print(file=myQueryFile)
        for row in cursorSqlServer:
            print(row, file=myQueryFile)
        cursorSqlServer.close() 
        myQueryFile.close()
        myFile.close()
        print("\x1b[1;33m"+"Reporte de consulta 1 generado exitosamente :D")
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al generar reporte :(')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...") 

def Query2():
    try:
        myFile = open("logs.txt", "a")
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")   
        print(date_time + " - Generando reporte de consulta #2", file=myFile) 
        cursorSqlServer = sqlServerDB.cursor()
        cursorSqlServer.execute('''SELECT T.year_field AS 'year', P.pais, T.valor FROM (
                                SELECT F.year_field, R.id_pais, R.valor FROM ( SELECT IP.id_pais, IP.valor, IP.id_fecha
                                    FROM [PROYECTO1].indicadorpais AS IP
                                    INNER JOIN [PROYECTO1].indicador AS I
                                    ON IP.id_indicador = I.id_indicador
                                    WHERE I.indicador LIKE 'Crecimiento del PIB%') AS R
                                INNER JOIN [PROYECTO1].fecha AS F ON R.id_fecha = F.id_fecha
                                WHERE F.year_field = 2018
                                ORDER BY R.valor ASC
                                OFFSET 0 ROWS
                                FETCH NEXT 10 ROWS ONLY) AS T
                                INNER JOIN [PROYECTO1].pais AS P
                                ON T.id_pais = P.id_pais''')
        myQueryFile = open("consulta2.txt", "w")
        print("------------ CONSULTA 2 ------------", file=myQueryFile)
        print(file=myQueryFile)
        for row in cursorSqlServer:
            print(row, file=myQueryFile)
        cursorSqlServer.close() 
        myQueryFile.close()
        myFile.close()
        print("\x1b[1;33m"+"Reporte de consulta 2 generado exitosamente :D")
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al generar reporte :(')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")

myApp = CLI()