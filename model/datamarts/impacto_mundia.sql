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

CREATE TABLE [PROYECTO1].pais_im(
    id_pais int not null identity(1,1),
    pais varchar(300),
    codigo_pais varchar(300),
    constraint pk_pais_im primary key (id_pais)
);

CREATE TABLE [PROYECTO1].anio_registrado_im(
    id_anio int not null identity(1,1),
    anio int,
    constraint pk_anio_registrado_im primary key (id_anio)
);

CREATE TABLE [PROYECTO1].registro_pibpais_im(
    cod_pais int not null,
    cod_anio int not null,
    medida_pib float,
    constraint pk_registro_pibpais_im primary key (cod_anio,cod_pais),
    constraint fk_registropibim_pais foreign key (cod_pais) references pais_im(id_pais),
    constraint fk_registropibim_anio foreign key (cod_anio) references anio_registrado_im(id_anio)
);