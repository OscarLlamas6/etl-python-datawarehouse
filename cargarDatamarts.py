# --------------- CARGAR DATAMART_INFLACION ---------------
# =========================================================

CARGAR_TABLA_DM_INFLACION_PAIS = ("""
insert into pais_inf(pais) (
    select distinct pais from [PROYECTO1].pais
);
""")

CARGAR_TABLA_DM_INFLACION_ANIO_REG = ("""
insert into anio_registrado_inf(anio) (
    select distinct year_field from [PROYECTO1].fecha
);
""")

CARGAR_TABLA_DM_INFLACION_REG_INF = ("""
insert into registro_inflacionpais_inf(cod_pais, cod_anio, medida_inflacion) (
    select pais_inf.id_pais, id_anio, vl 
    from pais_inf, anio_registrado_inf, (
        select indicador as ind, pais as ps, year_field as fch, valor as vl
        from [PROYECTO1].indicador, [PROYECTO1].pais,
        [PROYECTO1].fecha, [PROYECTO1].indicadorpais
        where [PROYECTO1].indicador.id_indicador = [PROYECTO1].indicadorpais.id_indicador
        and [PROYECTO1].pais.id_pais = [PROYECTO1].indicadorpais.id_pais
        and [PROYECTO1].fecha.id_fecha = [PROYECTO1].indicadorpais.id_fecha
    ) as tmp
    where pais_inf.pais = ps
    and anio = fch
    and ind = 'Inflación, precios al consumidor (% anual)'
);
""")

SCRIPTS_CARGA_DM_INFLACION = [CARGAR_TABLA_DM_INFLACION_PAIS,CARGAR_TABLA_DM_INFLACION_ANIO_REG,CARGAR_TABLA_DM_INFLACION_REG_INF]

# ----------- CARGAR DATAMART_IMPACTO_MUNDIAL ------------
# =========================================================

CARGAR_TABLA_DM_IMPACTO_PAIS = ("""
insert into pais_im(pais) (
    select distinct pais from [PROYECTO1].pais
);
""")

CARGAR_TABLA_DM_IMPACTO_ANIO = ("""
insert into anio_registrado_im(anio) (
    select distinct year_field from [PROYECTO1].fecha
);
""")

CARGAR_TABLA_DM_IMPACTO_REGISTROPIB = ("""
insert into registro_pibpais_im(cod_pais, cod_anio, medida_pib) (
    select pais_im.id_pais, id_anio, vl 
    from pais_im, anio_registrado_im, (
        select indicador as ind, pais as ps, year_field as fch, valor as vl
        from [PROYECTO1].indicador, [PROYECTO1].pais,
        [PROYECTO1].fecha, [PROYECTO1].indicadorpais
        where [PROYECTO1].indicador.id_indicador = [PROYECTO1].indicadorpais.id_indicador
        and [PROYECTO1].pais.id_pais = [PROYECTO1].indicadorpais.id_pais
        and [PROYECTO1].fecha.id_fecha = [PROYECTO1].indicadorpais.id_fecha
    ) as tmp
    where pais_im.pais = ps
    and anio = fch
    and ind = 'Crecimiento del PIB (% anual)'
);
""")

SCRIPTS_CARGA_DM_IMPACTO = [CARGAR_TABLA_DM_IMPACTO_PAIS,CARGAR_TABLA_DM_IMPACTO_ANIO,CARGAR_TABLA_DM_IMPACTO_REGISTROPIB]


# --------------- CARGAR DATAMART_COMBIANDO ---------------
# =========================================================

CARGAR_TABLA_DM_COMBINADO_PAIS = ("""
insert into pais_comb(pais) (
    select distinct pais from [PROYECTO1].pais
);
""")

CARGAR_TABLA_DM_COMBINADO_ANIO_REG = ("""
insert into anio_registrado_comb(anio) (
    select distinct year_field from [PROYECTO1].fecha
);
""")

CARGAR_TABLA_DM_COMBINADO_ = ("""
insert into estado_pais_comb(cod_pais, cod_anio, medida_pib,medida_inflacion) (
    select pais_comb.id_pais, id_anio, vl, vl1
    from pais_comb, 
    anio_registrado_comb, 
    (
        select indicador as ind, pais as ps, year_field as fch, valor as vl
        from [PROYECTO1].indicador, [PROYECTO1].pais,
        [PROYECTO1].fecha, [PROYECTO1].indicadorpais
        where [PROYECTO1].indicador.id_indicador = [PROYECTO1].indicadorpais.id_indicador
        and [PROYECTO1].pais.id_pais = [PROYECTO1].indicadorpais.id_pais
        and [PROYECTO1].fecha.id_fecha = [PROYECTO1].indicadorpais.id_fecha
        and [PROYECTO1].indicador.indicador = 'Crecimiento del PIB (% anual)'
    ) as tmp1,
    (
        select indicador as ind1, pais as ps1, year_field as fch1, valor as vl1
        from [PROYECTO1].indicador, [PROYECTO1].pais,
        [PROYECTO1].fecha, [PROYECTO1].indicadorpais
        where [PROYECTO1].indicador.id_indicador = [PROYECTO1].indicadorpais.id_indicador
        and [PROYECTO1].pais.id_pais = [PROYECTO1].indicadorpais.id_pais
        and [PROYECTO1].fecha.id_fecha = [PROYECTO1].indicadorpais.id_fecha
        and [PROYECTO1].indicador.indicador = 'Inflación, precios al consumidor (% anual)'
    ) as tmp2
    where pais_comb.pais = ps
    and pais_comb.pais = ps1
    and anio = fch
    and anio = fch1
    and ind = ind1
    and ps = ps1
    and fch = fch1
);
""")

SCRIPTS_CARGA_DM_COMBINADO = [CARGAR_TABLA_DM_COMBINADO_PAIS,CARGAR_TABLA_DM_COMBINADO_ANIO_REG,CARGAR_TABLA_DM_COMBINADO_]
