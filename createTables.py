5#TEMPORAL PIB

DROP_TEMPORAL_PIB=('DROP TABLE if exists temporalPib;')

DROP_TEMPORAL_PIB_MSSQL=('DROP TABLE if exists [PROYECTO1].temporalPib;')

CREATE_TEMPORAL_PIB=('CREATE TABLE temporalPib('
    'countryName VARCHAR(200),'
    'countryCode VARCHAR(50),'
    'indicatorName VARCHAR(200),'
    'indicatorCode VARCHAR(200),'
    'A1960 float,'
    'A1961 float,'
    'A1962 float,'
    'A1963 float,'
    'A1964 float,'
    'A1965 float,'
    'A1966 float,'
    'A1967 float,'
    'A1968 float,'
    'A1969 float,'
    'A1970 float,'
    'A1971 float,'
    'A1972 float,'
    'A1973 float,'
    'A1974 float,'
    'A1975 float,'
    'A1976 float,'
    'A1977 float,'
    'A1978 float,'
    'A1979 float,'
    'A1980 float,'
    'A1981 float,'
    'A1982 float,'
    'A1983 float,'
    'A1984 float,'
    'A1985 float,'
    'A1986 float,'
    'A1987 float,'
    'A1988 float,'
    'A1989 float,'
    'A1990 float,'
    'A1991 float,'
    'A1992 float,'
    'A1993 float,'
    'A1994 float,'
    'A1995 float,'
    'A1996 float,'
    'A1997 float,'
    'A1998 float,'
    'A1999 float,'
    'A2000 float,'
    'A2001 float,'
    'A2002 float,'
    'A2003 float,'
    'A2004 float,'
    'A2005 float,'
    'A2006 float,'
    'A2007 float,'
    'A2008 float,'
    'A2009 float,'
    'A2010 float,'
    'A2011 float,'
    'A2012 float,'
    'A2013 float,'
    'A2014 float,'
    'A2015 float,'
    'A2016 float,'
    'A2017 float,'
    'A2018 float,'
    'A2019 float,'
    'A2020 float,'
    'A2021 float' 
    ');')

CREATE_TEMPORAL_PIB_MSSQL=('CREATE TABLE [PROYECTO1].temporalPib('
    'countryName VARCHAR(200),'
    'countryCode VARCHAR(50),'
    'indicatorName VARCHAR(200),'
    'indicatorCode VARCHAR(200),'
    'A1960 float,'
    'A1961 float,'
    'A1962 float,'
    'A1963 float,'
    'A1964 float,'
    'A1965 float,'
    'A1966 float,'
    'A1967 float,'
    'A1968 float,'
    'A1969 float,'
    'A1970 float,'
    'A1971 float,'
    'A1972 float,'
    'A1973 float,'
    'A1974 float,'
    'A1975 float,'
    'A1976 float,'
    'A1977 float,'
    'A1978 float,'
    'A1979 float,'
    'A1980 float,'
    'A1981 float,'
    'A1982 float,'
    'A1983 float,'
    'A1984 float,'
    'A1985 float,'
    'A1986 float,'
    'A1987 float,'
    'A1988 float,'
    'A1989 float,'
    'A1990 float,'
    'A1991 float,'
    'A1992 float,'
    'A1993 float,'
    'A1994 float,'
    'A1995 float,'
    'A1996 float,'
    'A1997 float,'
    'A1998 float,'
    'A1999 float,'
    'A2000 float,'
    'A2001 float,'
    'A2002 float,'
    'A2003 float,'
    'A2004 float,'
    'A2005 float,'
    'A2006 float,'
    'A2007 float,'
    'A2008 float,'
    'A2009 float,'
    'A2010 float,'
    'A2011 float,'
    'A2012 float,'
    'A2013 float,'
    'A2014 float,'
    'A2015 float,'
    'A2016 float,'
    'A2017 float,'
    'A2018 float,'
    'A2019 float,'
    'A2020 float,'
    'A2021 float' 
    ');')

#TEMPORAL INFLACION

DROP_TEMPORAL_INFLACION=('DROP TABLE if exists temporalInflacion;')

DROP_TEMPORAL_INFLACION_MSSQL=('DROP TABLE if exists [PROYECTO1].temporalInflacion;')

CREATE_TEMPORAL_INFLACION=('CREATE TABLE temporalInflacion('
    'countryName VARCHAR(200),'
    'countryCode VARCHAR(50),'
    'indicatorName VARCHAR(200),'
    'indicatorCode VARCHAR(200),'
    'A1960 float,'
    'A1961 float,'
    'A1962 float,'
    'A1963 float,'
    'A1964 float,'
    'A1965 float,'
    'A1966 float,'
    'A1967 float,'
    'A1968 float,'
    'A1969 float,'
    'A1970 float,'
    'A1971 float,'
    'A1972 float,'
    'A1973 float,'
    'A1974 float,'
    'A1975 float,'
    'A1976 float,'
    'A1977 float,'
    'A1978 float,'
    'A1979 float,'
    'A1980 float,'
    'A1981 float,'
    'A1982 float,'
    'A1983 float,'
    'A1984 float,'
    'A1985 float,'
    'A1986 float,'
    'A1987 float,'
    'A1988 float,'
    'A1989 float,'
    'A1990 float,'
    'A1991 float,'
    'A1992 float,'
    'A1993 float,'
    'A1994 float,'
    'A1995 float,'
    'A1996 float,'
    'A1997 float,'
    'A1998 float,'
    'A1999 float,'
    'A2000 float,'
    'A2001 float,'
    'A2002 float,'
    'A2003 float,'
    'A2004 float,'
    'A2005 float,'
    'A2006 float,'
    'A2007 float,'
    'A2008 float,'
    'A2009 float,'
    'A2010 float,'
    'A2011 float,'
    'A2012 float,'
    'A2013 float,'
    'A2014 float,'
    'A2015 float,'
    'A2016 float,'
    'A2017 float,'
    'A2018 float,'
    'A2019 float,'
    'A2020 float,'
    'A2021 float' 
    ');')

CREATE_TEMPORAL_INFLACION_MSSQL=('CREATE TABLE [PROYECTO1].temporalInflacion('
    'countryName VARCHAR(200),'
    'countryCode VARCHAR(50),'
    'indicatorName VARCHAR(200),'
    'indicatorCode VARCHAR(200),'
    'A1960 float,'
    'A1961 float,'
    'A1962 float,'
    'A1963 float,'
    'A1964 float,'
    'A1965 float,'
    'A1966 float,'
    'A1967 float,'
    'A1968 float,'
    'A1969 float,'
    'A1970 float,'
    'A1971 float,'
    'A1972 float,'
    'A1973 float,'
    'A1974 float,'
    'A1975 float,'
    'A1976 float,'
    'A1977 float,'
    'A1978 float,'
    'A1979 float,'
    'A1980 float,'
    'A1981 float,'
    'A1982 float,'
    'A1983 float,'
    'A1984 float,'
    'A1985 float,'
    'A1986 float,'
    'A1987 float,'
    'A1988 float,'
    'A1989 float,'
    'A1990 float,'
    'A1991 float,'
    'A1992 float,'
    'A1993 float,'
    'A1994 float,'
    'A1995 float,'
    'A1996 float,'
    'A1997 float,'
    'A1998 float,'
    'A1999 float,'
    'A2000 float,'
    'A2001 float,'
    'A2002 float,'
    'A2003 float,'
    'A2004 float,'
    'A2005 float,'
    'A2006 float,'
    'A2007 float,'
    'A2008 float,'
    'A2009 float,'
    'A2010 float,'
    'A2011 float,'
    'A2012 float,'
    'A2013 float,'
    'A2014 float,'
    'A2015 float,'
    'A2016 float,'
    'A2017 float,'
    'A2018 float,'
    'A2019 float,'
    'A2020 float,'
    'A2021 float' 
    ');')

# --------------------------- DATAMARTS -------------------------------------
# ===========================================================================

# TABLAS DATAMART INFLACION =================================
CREAR_DATAMART_INFLACION = ("""
IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'datamart_inflacion')
BEGIN
    CREATE DATABASE [datamart_inflacion]
END
""")

CREAR_TABLA_PAIS_INF = ("""
CREATE TABLE pais_inf(
    id_pais int not null identity(1,1),
    pais varchar(300),
    codigo_pais varchar(300),
    constraint pk_pais_ing primary key (id_pais)
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
CREATE TABLE registro_inflacionpais_im(
    cod_pais int not null,
    cod_anio int not null,
    medida_inflacion float,
    constraint pk_registro_inflacionpais_im primary key (cod_anio,cod_pais),
    constraint fk_registroinflacion_pais foreign key (cod_pais) references pais_inf(id_pais),
    constraint fk_registroinflacion_anio foreign key (cod_anio) references anio_registrado_inf(id_anio)
);
""")

SCRIPTS_DATAMART_INFLACION = [CREAR_DATAMART_INFLACION,CREAR_TABLA_PAIS_INF,CREAR_TABLA_ANIO_REGISTRADO_INF,CREAR_TABLA_REGISTRO_INFLACIONPAIS_INF]

# TABLAS DATAMART IMPACTO_MUNDIAL =================================
CREAR_DATAMART_IMPACTO_MUNDIAL = ("""
IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'datamart_impacto')
BEGIN
    CREATE DATABASE [datamart_impacto]
END
""")

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
    constraint fk_registropib_pais foreign key (cod_pais) references pais_im(id_pais),
    constraint fk_registropib_anio foreign key (cod_anio) references anio_registrado_im(id_anio)
);
""")

SCRIPTS_DATAMART_IMPACTO = [CREAR_DATAMART_IMPACTO_MUNDIAL,CREAR_TABLA_PAIS_IM,CREAR_TABLA_ANIO_REGISTRADO_IM,CREAR_TABLA_REGISTRO_PIBPAIS_IM]

# TABLAS DATAMART COMBINADO =================================
CREAR_DATAMART_COMBINADO = ("""
IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'datamart_combinado')
BEGIN
    CREATE DATABASE [datamart_combinado]
END
""")

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
    constraint pk_estado_pais_comp primary key (cod_pais, cod_anio),
    constraint fk_estadopais_pais foreign key (cod_pais) references pais_comb(id_pais),
    constraint fk_estadopais_anio foreign key (cod_anio) references anio_registrado_comb(id_anio)
);
""")

SCRIPTS_DATAMART_COMBINADO = [CREAR_DATAMART_COMBINADO,CREAR_TABLA_PAIS_COMB,CREAR_TABLA_ANIO_REGISTRADO_COMB,CREAR_TABLA_ESTADO_PAISCOMP]
