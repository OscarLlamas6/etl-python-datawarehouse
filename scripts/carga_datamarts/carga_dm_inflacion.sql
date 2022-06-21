insert into pais_inf(pais) (
    select distinct pais from [PROYECTO1].pais
);

insert into anio_registrado_inf(anio) (
    select distinct year_field from [PROYECTO1].fecha
);

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