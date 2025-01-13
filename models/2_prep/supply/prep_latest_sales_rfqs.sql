with 
sales_rfq_per_order as ( 
    select 
        docs.order_uuid, 
        docs.created, 
        sales_rfqs.quote_uuid, 
        sales_rfqs.application, 
        sales_rfqs.details, 
        sales_rfqs.delivered_by, 
        row_number() over (partition by order_uuid order by created desc) as rn 
        from {{ ref('network_services', 'gold_sales_rfqs') }} sales_rfqs 
        inner join {{ ref('documents') }} as docs on docs.uuid = sales_rfqs.quote_uuid 
    group by 1,2,3,4,5,6 ) 
    
select * from sales_rfq_per_order where rn = 1