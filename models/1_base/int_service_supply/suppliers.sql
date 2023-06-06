{% set boolean_fields = [
       "send_automatic_rfq",
       "allow_for_rfq"
    ]
%}

select id,
       address_id,
       name,
       tax_number,
       created,
       updated,
       deleted,
       currency_code,
       unit_preference,
       monthly_order_value_target,
       tax_number_2,
       default_shipping_carrier_id,
       case when is_eligible_for_virtual_quality_control = 'true' then True else False end as is_eligible_for_vqc,
       decode(suspended, 'false', False, 'true', True)          as is_suspended,
       decode(accepts_auctions, 'false', False, 'true', True)   as is_accepting_auctions,
       {% for boolean_field in boolean_fields %}
           {{ varchar_to_boolean(boolean_field) }}
           {% if not loop.last %},{% endif %}
       {% endfor %}

from {{ source('int_service_supply', 'suppliers') }}