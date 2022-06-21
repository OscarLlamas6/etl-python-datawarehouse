insert into pais_comb(pais) (
    select distinct pais from [PROYECTO1].pais
);

insert into anio_registrado_comb(anio) (
    select distinct year_field from [PROYECTO1].fecha
);

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

