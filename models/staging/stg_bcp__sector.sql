with source as (
    select * from {{ source('raw_bcp', 'raw_sector') }}
),

filtrado as (
    select * from source
    where Fecha not like '%Total%'
      and Fecha not like '%TOTAL%'
      and length(Fecha) = 7
),

unpivoted as (
    unpivot filtrado
    on Atlas, Bancop, Basa, BNA, BNF, Citibank, Continental,
       "Do Brasil", Familiar, GNB, "GNB Fusión", Interfisa,
       Itaú, Regional, Río, Solar, Sudameris, UENO, Visión, Zeta
    into
        name  banco
        value credito
),

final as (
    select
        cast(strptime(Fecha, '%Y/%m') as date) as fecha,
        "Sector E."                            as sector,
        Moneda                                 as moneda,
        banco,
        cast(credito as double)                as credito_millones
    from unpivoted
    where credito > 0
)

select * from final
