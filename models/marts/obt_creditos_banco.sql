with sector as (
    select * from {{ ref('stg_bcp__sector') }}
),

actividad as (
    select * from {{ ref('stg_bcp__actividad') }}
),

sector_con_actividad as (
    select
        s.fecha,
        year(s.fecha)              as anio,
        month(s.fecha)             as mes,
        s.moneda,
        s.banco,
        s.sector,
        s.credito_millones         as credito_sector,
        s.credito_millones / nullif(sum(s.credito_millones)
            over (partition by s.fecha, s.moneda), 0) * 100
                                   as participacion_pct
    from sector s
)

select * from sector_con_actividad