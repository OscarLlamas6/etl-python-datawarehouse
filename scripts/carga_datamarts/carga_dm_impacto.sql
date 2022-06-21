insert into pais_im(pais) (
    select distinct pais from [PROYECTO1].pais
);

insert into anio_registrado_im(anio) (
    select distinct year_field from [PROYECTO1].fecha
);

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