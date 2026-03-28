with source as (
    select * from {{ source('raw_bcp', 'raw_cartera') }}
),

unpivoted as (
    unpivot source
    on Atlas, Bancop, Basa, BNA, BNF, Citibank, Continental,
       "Do Brasil", Familiar, GNB, "GNB Fusión", Interfisa,
       Itaú, Regional, Río, Solar, Sudameris, UENO, Visión, Zeta
    into
        name  banco
        value monto
),

final as (
    select
        strptime(Fecha, '%Y/%m') + interval '0' day  as fecha,
        Clasificación                                 as clasificacion,
        Rubro                                         as rubro,
        Subrubro                                      as subrubro,
        Moneda                                        as moneda,
        banco,
        cast(monto as double)                         as monto_millones
    from unpivoted
    where monto > 0
)

select * from final