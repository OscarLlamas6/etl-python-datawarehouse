# --------------------------- DATAMARTS -------------------------------------
# ===========================================================================

CREAR_TABLA_PAIS_INF = ("""
CREATE TABLE [PROYECTO1].pais_inf(
    id_pais int not null identity(1,1),
    pais varchar(300),
    constraint pk_pais_inf primary key (id_pais)
);
""")

CREAR_TABLA_ANIO_REGISTRADO_INF = ("""
CREATE TABLE [PROYECTO1].anio_registrado_inf(
    id_anio int not null identity(1,1),
    anio int,
    constraint pk_anio_registrado_inf primary key (id_anio)
);
""")

CREAR_TABLA_REGISTRO_INFLACIONPAIS_INF = ("""
CREATE TABLE [PROYECTO1].registro_inflacionpais_inf(
    cod_pais int not null,
    cod_anio int not null,
    medida_inflacion float,
    constraint pk_registro_inflacionpais_inf primary key (cod_anio,cod_pais),
    constraint fk_registroinflacion_pais foreign key (cod_pais) references [PROYECTO1].pais_inf(id_pais),
    constraint fk_registroinflacion_anio foreign key (cod_anio) references [PROYECTO1].anio_registrado_inf(id_anio)
);
""")

SCRIPTS_DATAMART_INFLACION = [CREAR_TABLA_PAIS_INF,CREAR_TABLA_ANIO_REGISTRADO_INF,CREAR_TABLA_REGISTRO_INFLACIONPAIS_INF]

DROP_TABLA_DM_REG_INFLACIONPAIS = ("""
DROP TABLE if exists [PROYECTO1].registro_inflacionpais_inf ;
""")

DROP_TABLA_DM_INLFACION_ANIO = ("""
DROP TABLE if exists [PROYECTO1].anio_registrado_inf ;
""")

DROP_TABLA_DM_INFLACION_PAIS = ("""
DROP TABLE if exists [PROYECTO1].pais_inf ;
""")

SCRIPTS_DROP_DATAMART_INFLACION = [DROP_TABLA_DM_REG_INFLACIONPAIS,DROP_TABLA_DM_INLFACION_ANIO,DROP_TABLA_DM_INFLACION_PAIS]

# ================================= TABLAS DATAMART IMPACTO_MUNDIAL =================================

CREAR_TABLA_PAIS_IM = ("""
CREATE TABLE [PROYECTO1].pais_im(
    id_pais int not null identity(1,1),
    pais varchar(300),
    constraint pk_pais_im primary key (id_pais)
);
""")

CREAR_TABLA_ANIO_REGISTRADO_IM = ("""
CREATE TABLE [PROYECTO1].anio_registrado_im(
    id_anio int not null identity(1,1),
    anio int,
    constraint pk_anio_registrado_im primary key (id_anio)
);
""")

CREAR_TABLA_REGISTRO_PIBPAIS_IM = ("""
CREATE TABLE [PROYECTO1].registro_pibpais_im(
    cod_pais int not null,
    cod_anio int not null,
    medida_pib float,
    constraint pk_registro_pibpais_im primary key (cod_anio,cod_pais),
    constraint fk_registropibim_pais foreign key (cod_pais) references [PROYECTO1].pais_im(id_pais),
    constraint fk_registropibim_anio foreign key (cod_anio) references [PROYECTO1].anio_registrado_im(id_anio)
);
""")

SCRIPTS_DATAMART_IMPACTO = [CREAR_TABLA_PAIS_IM,CREAR_TABLA_ANIO_REGISTRADO_IM,CREAR_TABLA_REGISTRO_PIBPAIS_IM]

DROP_TABLA_DM_IMPACTO_REGISTROPIB = ("""
DROP TABLE if exists [PROYECTO1].registro_pibpais_im ;
""")

DROP_TABLA_DM_IMPACTO_ANIO = ("""
DROP TABLE if exists [PROYECTO1].anio_registrado_im ;
""")

DROP_TABLA_DM_IMPACTO_IMPACTO_PAIS = ("""
DROP TABLE if exists [PROYECTO1].pais_im ;
""")

SCRIPTS_DROP_DATAMART_IMPACTO = [DROP_TABLA_DM_IMPACTO_REGISTROPIB,DROP_TABLA_DM_IMPACTO_ANIO,DROP_TABLA_DM_IMPACTO_IMPACTO_PAIS]

# ================================= TABLAS DATAMART COMBINADO =================================


CREAR_TABLA_PAIS_COMB = ("""
CREATE TABLE [PROYECTO1].pais_comb(
    id_pais int not null identity(1,1),
    pais varchar(300),
    constraint pk_pais_comb primary key (id_pais)
);
""")

CREAR_TABLA_ANIO_REGISTRADO_COMB = ("""
CREATE TABLE [PROYECTO1].anio_registrado_comb(
    id_anio int not null identity(1,1),
    anio int,
    constraint pk_anio_registrado_comb primary key (id_anio)
);
""")

CREAR_TABLA_ESTADO_PAISCOMP = ("""
CREATE TABLE [PROYECTO1].estado_pais_comb(
    cod_pais int not null,
    cod_anio int not null,
    medida_pib float,
    medida_inflacion float,
    constraint pk_estado_pais_comb primary key (cod_pais, cod_anio),
    constraint fk_estadopaiscomb_pais foreign key (cod_pais) references [PROYECTO1].pais_comb(id_pais),
    constraint fk_estadopaiscomb_anio foreign key (cod_anio) references [PROYECTO1].anio_registrado_comb(id_anio)
);
""")

SCRIPTS_DATAMART_COMBINADO = [CREAR_TABLA_PAIS_COMB,CREAR_TABLA_ANIO_REGISTRADO_COMB,CREAR_TABLA_ESTADO_PAISCOMP]

DROP_TABLA_DM_COMBINADO_ESTADOPAIS = ("""
DROP TABLE if exists [PROYECTO1].estado_pais_comb ;
""")

DROP_TABLA_DM_COMBINADO_ANIO = ("""
DROP TABLE if exists [PROYECTO1].anio_registrado_comb ;
""")

DROP_TABLA_DM_COMBINADO_PAIS = ("""
DROP TABLE if exists [PROYECTO1].pais_comb ;
""")

SCRIPTS_DROP_DATAMART_COMBINADO = [DROP_TABLA_DM_COMBINADO_ESTADOPAIS,DROP_TABLA_DM_COMBINADO_ANIO,DROP_TABLA_DM_COMBINADO_PAIS]

