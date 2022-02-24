--Maintained by: Daniel
--Last updated: Feb 2022

-- This table provides an overview of the billing statements of the manufacturing suppliers.
-- The billing statement shows the amount for which MPs can invoice Hubs the amount comprises of orders completed in the previous month.
-- Sub total amount in USD is not provided as this can be derived from the purchase orders connected to the billing statement.

select 
       sbr.id,
       sbr.supplier_id,
       sbr.billing_month,
       sbr.invoice_uploaded_at,
       sbr.paid_out_at,
       sbr.reference_number,
       sbr.status,
       sbr.currency_code,
       coalesce(sbr.override_amount, sbr.amount) as sub_total_amount, -- local currency
       coalesce(sbr.override_tax_amount, sbr.tax_amount) as tax_amount, -- local currency
       sbr.override_amount is not null as is_over_written,
       sbr.override_reason,
       sc.name as corporate_country

from {{ source('int_service_supply', 'billing_requests') }} as sbr
left join {{ source('int_service_supply', 'company_entities') }} as sce on sbr.company_entity_id = sce.id
left join {{ source('int_service_supply', 'countries') }} as sc on sce.corporate_country_id = sc.country_id
where true