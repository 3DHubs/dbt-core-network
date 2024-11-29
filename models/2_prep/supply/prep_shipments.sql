select 
    *,
    -- Used to standardize the carrier names to prevent DHL Germany, DHL Express, DHL ground etc.
    case
        when upper(tracking_carrier_name) = 'DHL EXPRESS' then 'DHL EXPRESS'
        when upper(tracking_carrier_name) = 'DEUTSCHE POST DHL' then 'DHL EXPRESS'
        when upper(tracking_carrier_name) = 'DHL EXPRESS (PIECE ID)' then 'DHL EXPRESS'
        when upper(tracking_carrier_name) = 'DHL ACTIVE TRACING' then 'DHL EXPRESS'
        when upper(tracking_carrier_name) = 'DHL GLOBAL FORWARDING' then 'DHL EXPRESS'
        when upper(tracking_carrier_name) = 'DHL 2-MANN-HANDLING' then 'DHL EXPRESS'
        when upper(tracking_carrier_name) = 'DHL FREIGHT' then 'DHL EXPRESS'
        when upper(tracking_carrier_name) = 'DHL HONG KONG' then 'DHL EXPRESS'
        when upper(tracking_carrier_name) = 'DHL ECOMMERCE ASIA' then 'DHL EXPRESS'
        when upper(tracking_carrier_name) = 'DHL POLAND DOMESTIC' then 'DHL PARCEL'
        when upper(tracking_carrier_name) = 'DHL PARCEL NL' then 'DHL PARCEL'
        when upper(tracking_carrier_name) = 'DHL NETHERLANDS' then 'DHL PARCEL'
        when upper(tracking_carrier_name) = 'DHL BENELUX' then 'DHL PARCEL'
        when upper(tracking_carrier_name) = 'DHL ECOMMERCE US' then 'DHL PARCEL'
        when upper(tracking_carrier_name) = 'DHL PARCEL SPAIN' then 'DHL PARCEL'
        when upper(tracking_carrier_name) like '%FEDEX%' then 'FEDEX'
        when upper(tracking_carrier_name) like '%USPS%' then 'USPS'
        when upper(tracking_carrier_name) like '%UPS%' then 'UPS'
        when upper(tracking_carrier_name) like '%DPD%' then 'DPD'
        when upper(tracking_carrier_name) like '%POSTNL%' then 'POSTNL'
        when upper(tracking_carrier_name) like '%GLS%' then 'GLS'
        when upper(tracking_carrier_name) like '%TNT%' then 'TNT'
        when upper(tracking_carrier_name) like '%AUSTRALIA POST%' then 'AUSTRALIA POST'
        else tracking_carrier_name
    end as carrier_name_mapped

from {{ ref('shipments') }}