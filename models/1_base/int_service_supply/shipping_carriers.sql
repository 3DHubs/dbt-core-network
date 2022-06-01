select 
    sc.*,
    -- Used to standardize the carrier names to prevent DHL Germany, DHL Express, DHL ground etc.
    case
        when upper(sc.name) = 'DHL EXPRESS' then 'DHL EXPRESS'
        when upper(sc.name) = 'DEUTSCHE POST DHL' then 'DHL EXPRESS'
        when upper(sc.name) = 'DHL EXPRESS (PIECE ID)' then 'DHL EXPRESS'
        when upper(sc.name) = 'DHL ACTIVE TRACING' then 'DHL EXPRESS'
        when upper(sc.name) = 'DHL GLOBAL FORWARDING' then 'DHL EXPRESS'
        when upper(sc.name) = 'DHL 2-MANN-HANDLING' then 'DHL EXPRESS'
        when upper(sc.name) = 'DHL FREIGHT' then 'DHL EXPRESS'
        when upper(sc.name) = 'DHL HONG KONG' then 'DHL EXPRESS'
        when upper(sc.name) = 'DHL ECOMMERCE ASIA' then 'DHL EXPRESS'
        when upper(sc.name) = 'DHL POLAND DOMESTIC' then 'DHL PARCEL'
        when upper(sc.name) = 'DHL PARCEL NL' then 'DHL PARCEL'
        when upper(sc.name) = 'DHL NETHERLANDS' then 'DHL PARCEL'
        when upper(sc.name) = 'DHL BENELUX' then 'DHL PARCEL'
        when upper(sc.name) = 'DHL ECOMMERCE US' then 'DHL PARCEL'
        when upper(sc.name) = 'DHL PARCEL SPAIN' then 'DHL PARCEL'
        when upper(sc.name) like '%FEDEX%' then 'FEDEX'
        when upper(sc.name) like '%USPS%' then 'USPS'
        when upper(sc.name) like '%UPS%' then 'UPS'
        when upper(sc.name) like '%DPD%' then 'DPD'
        when upper(sc.name) like '%POSTNL%' then 'POSTNL'
        when upper(sc.name) like '%GLS%' then 'GLS'
        when upper(sc.name) like '%TNT%' then 'TNT'
        when upper(sc.name) like '%AUSTRALIA POST%' then 'AUSTRALIA POST'
        else sc.name
    end as carrier_name_mapped

from {{ source('int_service_supply', 'shipping_carriers') }} as sc