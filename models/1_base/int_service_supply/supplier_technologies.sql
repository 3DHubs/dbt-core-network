{% set boolean_fields = [
       "allow_orders_with_custom_finishes",
       "allow_cosmetic_worthy_finishes"
    ]
%}

select supplier_id,
       technology_id,
       max_active_orders,
       max_order_amount,
       strategic_orders_priority,
       min_order_amount,
       min_lead_time,
       num_parts_min,
       num_parts_max,
       num_units_min,
       num_units_max,
       {% for boolean_field in boolean_fields %}
           {{ varchar_to_boolean(boolean_field) }}
           {% if not loop.last %},{% endif %}
       {% endfor %},
       {{ dbt_utils.surrogate_key(['technology_id',
                                   'allow_orders_with_custom_finishes',
                                   'allow_cosmetic_worthy_finishes',
                                   'strategic_orders_priority',
                                   'min_order_amount',
                                   'max_order_amount']) }}                     as _supplier_attr_sk -- This surrogate key is used in snapshots to identify changes

from {{ source('int_service_supply', 'supplier_technologies') }}