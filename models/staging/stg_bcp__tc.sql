{{ config(materialized='table') }}

with source as (
    select * from {{ source('raw_bcp', 'raw_tc') }}
),

renamed as (
    select
        -- Convertimos el timestamp/texto al primer día del mes para el JOIN
        date_trunc('month', "Fecha_cierre"::date)::date as fecha_mes,
        Peso as tc_peso,
        Real as tc_real,
        USD as tc_usd,
        Euro as tc_euro
    from source
)

select * from renamed