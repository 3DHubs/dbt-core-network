--Last updated: 25 Oct 2024

-- This table provides an overview of the billing statements of the manufacturing suppliers
-- The billing statement shows the amount for which MPs can invoice Hubs the amount comprises of orders completed in the previous month
-- Sub total amount in USD is not provided as this can be derived from the purchase orders connected to the billing statement

select 
       id,
       supplier_id,
       billing_month,
       invoice_uploaded_at,
       paid_out_at,
       reference_number,
       status,
       currency_code,
       sub_total_amount, -- local currency
       tax_amount, -- local currency
       is_over_written,
       override_reason,
       corporate_country

from {{ ref('network_services', 'gold_billing_requests') }}