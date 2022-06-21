# --------------------------- DATAMARTS -------------------------------------
# ===========================================================================

# TABLAS DATAMART INFLACION =================================
# CREAR_DATAMART_INFLACION = ("""
# IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'datamart_inflacion')
# BEGIN
#     CREATE DATABASE [datamart_inflacion]
# END
# """)

CREAR_TABLA_PAIS_INF = ("""
CREATE TABLE pais_inf(
    id_pais int not null identity(1,1),
    pais varchar(300),
    codigo_pais varchar(300),
    constraint pk_pais_inf primary key (id_pais)
);
""")

CREAR_TABLA_ANIO_REGISTRADO_INF = ("""
CREATE TABLE anio_registrado_inf(
    id_anio int not null identity(1,1),
    anio int,
    constraint pk_anio_registrado_inf primary key (id_anio)
);
""")

CREAR_TABLA_REGISTRO_INFLACIONPAIS_INF = ("""
CREATE TABLE registro_inflacionpais_inf(
    cod_pais int not null,
    cod_anio int not null,
    medida_inflacion float,
    constraint pk_registro_inflacionpais_inf primary key (cod_anio,cod_pais),
    constraint fk_registroinflacion_pais foreign key (cod_pais) references pais_inf(id_pais),
    constraint fk_registroinflacion_anio foreign key (cod_anio) references anio_registrado_inf(id_anio)
);
""")

SCRIPTS_DATAMART_INFLACION = [CREAR_TABLA_PAIS_INF,CREAR_TABLA_ANIO_REGISTRADO_INF,CREAR_TABLA_REGISTRO_INFLACIONPAIS_INF]

DROP_TABLA_DM_REG_INFLACIONPAIS = ("""
DROP TABLE if exists registro_inflacionpais_inf ;
""")

DROP_TABLA_DM_INLFACION_ANIO = ("""
DROP TABLE if exists anio_registrado_inf ;
""")

DROP_TABLA_DM_INFLACION_PAIS = ("""
DROP TABLE if exists pais_inf ;
""")

SCRIPTS_DROP_DATAMART_INFLACION = [DROP_TABLA_DM_REG_INFLACIONPAIS,DROP_TABLA_DM_INLFACION_ANIO,DROP_TABLA_DM_INFLACION_PAIS]

# TABLAS DATAMART IMPACTO_MUNDIAL =================================
# CREAR_DATAMART_IMPACTO_MUNDIAL = ("""
# IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'datamart_impacto')
# BEGIN
#     CREATE DATABASE [datamart_impacto]
# END
# """)

CREAR_TABLA_PAIS_IM = ("""
CREATE TABLE pais_im(
    id_pais int not null identity(1,1),
    pais varchar(300),
    codigo_pais varchar(300),
    constraint pk_pais_im primary key (id_pais)
);
""")

CREAR_TABLA_ANIO_REGISTRADO_IM = ("""
CREATE TABLE anio_registrado_im(
    id_anio int not null identity(1,1),
    anio int,
    constraint pk_anio_registrado_im primary key (id_anio)
);
""")

CREAR_TABLA_REGISTRO_PIBPAIS_IM = ("""
CREATE TABLE registro_pibpais_im(
    cod_pais int not null,
    cod_anio int not null,
    medida_pib float,
    constraint pk_registro_pibpais_im primary key (cod_anio,cod_pais),
    constraint fk_registropibim_pais foreign key (cod_pais) references pais_im(id_pais),
    constraint fk_registropibim_anio foreign key (cod_anio) references anio_registrado_im(id_anio)
);
""")

SCRIPTS_DATAMART_IMPACTO = [CREAR_TABLA_PAIS_IM,CREAR_TABLA_ANIO_REGISTRADO_IM,CREAR_TABLA_REGISTRO_PIBPAIS_IM]

DROP_TABLA_DM_IMPACTO_REGISTROPIB = ("""
DROP TABLE if exists registro_pibpais_im ;
""")

DROP_TABLA_DM_IMPACTO_ANIO = ("""
DROP TABLE if exists anio_registrado_im ;
""")

DROP_TABLA_DM_IMPACTO_IMPACTO_PAIS = ("""
DROP TABLE if exists pais_im ;
""")

SCRIPTS_DROP_DATAMART_IMPACTO = [DROP_TABLA_DM_IMPACTO_REGISTROPIB,DROP_TABLA_DM_IMPACTO_ANIO,DROP_TABLA_DM_IMPACTO_IMPACTO_PAIS]

# TABLAS DATAMART COMBINADO =================================
# CREAR_DATAMART_COMBINADO = ("""
# IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'datamart_combinado')
# BEGIN
#     CREATE DATABASE [datamart_combinado]
# END
# """)

CREAR_TABLA_PAIS_COMB = ("""
CREATE TABLE pais_comb(
    id_pais int not null identity(1,1),
    pais varchar(300),
    codigo_pais varchar(300),
    constraint pk_pais_comb primary key (id_pais)
);
""")

CREAR_TABLA_ANIO_REGISTRADO_COMB = ("""
CREATE TABLE anio_registrado_comb(
    id_anio int not null identity(1,1),
    anio int,
    constraint pk_anio_registrado_comb primary key (id_anio)
);
""")

CREAR_TABLA_ESTADO_PAISCOMP = ("""
CREATE TABLE estado_pais_comb(
    cod_pais int not null,
    cod_anio int not null,
    medida_pib float,
    medida_inflacion float,
    constraint pk_estado_pais_comb primary key (cod_pais, cod_anio),
    constraint fk_estadopaiscomb_pais foreign key (cod_pais) references pais_comb(id_pais),
    constraint fk_estadopaiscomb_anio foreign key (cod_anio) references anio_registrado_comb(id_anio)
);
""")

SCRIPTS_DATAMART_COMBINADO = [CREAR_TABLA_PAIS_COMB,CREAR_TABLA_ANIO_REGISTRADO_COMB,CREAR_TABLA_ESTADO_PAISCOMP]

DROP_TABLA_DM_COMBINADO_ESTADOPAIS = ("""
DROP TABLE if exists estado_pais_comb ;
""")

DROP_TABLA_DM_COMBINADO_ANIO = ("""
DROP TABLE if exists anio_registrado_comb ;
""")

DROP_TABLA_DM_COMBINADO_PAIS = ("""
DROP TABLE if exists pais_comb ;
""")

SCRIPTS_DROP_DATAMART_COMBINADO = [DROP_TABLA_DM_COMBINADO_ESTADOPAIS,DROP_TABLA_DM_COMBINADO_ANIO,DROP_TABLA_DM_COMBINADO_PAIS]

