USE [master]

IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'PROYECTO1')
BEGIN
    CREATE DATABASE [PROYECTO1]
END

USE [PROYECTO1]

IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'PROYECTO1'))
BEGIN
    EXEC ('CREATE SCHEMA [PROYECTO1]')
END

CREATE TABLE  [PROYECTO1].indicador(
    id_indicador int not null identity(1,1),
    indicador varchar(60),
    codigo_indicador varchar(60),
    constraint pk_indicador primary key(id_indicador)
);

CREATE TABLE [PROYECTO1].pais(
    id_pais int not null identity(1,1),
    pais varchar(60),
    codigo_pais varchar(60),
    constraint pk_pais primary key (id_pais)
);

CREATE TABLE [PROYECTO1].fecha(
    id_fecha int not null identity(1,1),
    year_field int not null,
    constraint pk_fecha primary key (id_fecha)
);

CREATE TABLE [PROYECTO1].indicadorpais(
    id_indicadorPais int not null identity(1,1),
    id_pais int not null,
    id_indicador int not null,
    id_fecha int not null,
    valor float,
    constraint pk_indicadorpais primary key (id_indicadorPais),
    constraint fk_indicadorpais_pais foreign key (id_pais) references [PROYECTO1].pais(id_pais),
    constraint fk_indicadorpais_indicador foreign key (id_indicador) references [PROYECTO1].indicador(id_indicador),
    constraint fk_indicadorpais_fecha foreign key (id_fecha) references [PROYECTO1].fecha(id_fecha)
);