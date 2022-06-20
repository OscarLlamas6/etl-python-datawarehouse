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

CREATE TABLE [PROYECTO1].indicador(
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

CREATE TABLE [PROYECTO1].indicador_pais(
    cod_pais int not null,
    cod_indicador int not null,
    year_field int not null,
    valor float,
    constraint pk_indicador_pais primary key (cod_pais, cod_indicador, year_field),
    constraint fk_indicadorpais_pais foreign key (cod_pais) references pais(id_pais),
    constraint fk_indicadorpais_indicador foreign key (cod_indicador) references indicador(id_indicador)
);

