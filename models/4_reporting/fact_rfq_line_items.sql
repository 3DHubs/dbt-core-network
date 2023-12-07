{{
    config(
        post_hook = "analyze {{ this }}",

    )
}}

-- This model queries from the underlying line items model to include rfq line item information only

 
select  

        docs.order_uuid,
        li.uuid               as line_item_uuid,
        docs.uuid             as document_uuid,
        docs.type             as document_type,
        docs.revision         as document_revision,
        bids.auction_uuid,
        bids.supplier_id,
        md5(supplier_id || bids.auction_uuid) as supplier_rfq_uuid,

        -- Supply Line Items Base Fields
        li.created                                                                 as created_date,
        li.li_updated_at,
        li.id                                                                      as line_item_id,
        case when li.type = 'part' then
        row_number() over (partition by li.quote_uuid, li.type order by li.title,li.id asc)
        else null end                                                              as line_item_number,
        li.quote_uuid,
        li.material_id,
        li.material_subset_id,
        li.process_id,
        li.material_color_id,
        li.branded_material_id,
        li.shipping_option_id,
        li.type                                                                    as line_item_type,
        li.custom_material_subset_name                                             as custom_material_subset_name,
        (nullif(li.custom_material_subset_name, '') is not null)                   as has_custom_material_subset,
        li.custom_finish_name                                                      as custom_surface_finish_name,
        (nullif(li.custom_finish_name, '') is not null)                            as has_custom_finish,
        li.description                                                             as line_item_description, -- comment from customer 
        (nullif(li.description, '') is not null)                                   as has_customer_note,
        li.admin_description,
        li.title                                                                   as line_item_title,       -- by default set to model file name; custom line_items: set by admin
        li.has_technical_drawings,
        li.lead_time_options,
        li.part_orientation_additional_notes,
        li.part_orientation_vector,
        li.upload_properties,
        li.unit,
        li.quantity,
        li.has_threads,
        li.has_fits,
        li.has_internal_corners,
        li.has_part_marking,
        li.infill,
        li.layer_height,
        li.is_cosmetic,
        -- Tolerances
        t.name                                                                       as tiered_tolerance,
        li.general_tolerance_class                                                   as general_tolerance,
        li.custom_tolerance,
        li.custom_tolerance_unit,

        -- Materials, Processes & Finishes
        mat.name                                                                     as material_name,
        mt.name                                                                      as material_type_name,
        msub.name                                                                    as material_subset_name,
        msub.density                                                                 as material_density_g_cm3,
        mc.name                                                                      as material_color_name,
        prc.name                                                                     as process_name,
        bmat.name                                                                    as branded_material_name,
        mf.name                                                                      as surface_finish_name,
        mf.cosmetic_type,

        -- Amount Fields
        li.auto_price_amount,
        case
        --          If price amount is given always use this as it is the manually set amount
        when li.price_amount is not null then
        li.price_amount

        --          Some non part line items have no unit price, thus we use auto_price amount (e.g. such as surcharge)
        when li.type != 'part' and li.auto_price_amount is not null then
            coalesce(li.auto_price_amount, 0)

        --          When unit price amount is given a simple multiplication with the quantity (if 0 then 1) will do (both parts and non parts), if the
        --          order is of technology injection molding then we also add in tooling
        when li.unit_price_amount is not null then
        coalesce(li.unit_price_amount::double precision * coalesce(nullif(li.quantity, 0), 1) +
        coalesce(li.tooling_price_amount, li.auto_tooling_price_amount, 0),0)

        --          For all other line items auto_price_amount is given but still requires to be rounded appropriately.
        --          Unit prices should have no more decimals than 2, therefore, the auto_price_amount for the total line item is
        --          divided by the quantity and then rounded through the banker rounding method before multiplier again with the
        --          Quantity this ensures that the unit price is within 2 decimals and that the total is equal to unit price * q
        else

        case when abs(cast((li.auto_price_amount * docs.price_multiplier)/coalesce(nullif(li.quantity, 0), 1) as int) -
                        (li.auto_price_amount * docs.price_multiplier)/coalesce(nullif(li.quantity, 0), 1)) = 0.5 then
                round((li.auto_price_amount * docs.price_multiplier)/coalesce(nullif(li.quantity, 0), 1)/2,0)*2
        else round((li.auto_price_amount * docs.price_multiplier)/coalesce(nullif(li.quantity, 0),1),0)
        end * coalesce(nullif(li.quantity, 0), 1)

        end  / 100.00                                                               as line_item_price_amount,
        line_item_price_amount / rates.rate                                         as line_item_price_amount_usd,

        docs.currency_code                                                          as line_item_price_amount_source_currency,
        -- These amount fields are only manually inserted, nowadays only unit_price_amount is populated and the price_amount is calculated from the quantity
        coalesce(li.unit_price_amount, li.price_amount) is not null                 as line_item_price_amount_manually_edited

    from {{ ref('line_items') }} as li
             left join {{ ref('prep_supply_documents') }} as docs on docs.uuid = li.quote_uuid
             inner join {{ ref('prep_bids')}} as bids on bids.uuid = docs.uuid           
             inner join {{ ref('prep_auctions')}} as auction_rfq on auction_rfq.auction_uuid = bids.auction_uuid and auction_rfq.is_rfq      

             -- Materials Processes and Finishes
             left join {{ ref('materials') }} as mat on mat.material_id = li.material_id
             left join {{ source('int_service_supply', 'material_types') }}  as mt on mt.material_type_id = mat.material_type_id
             left join {{ ref('processes') }} as prc on prc.process_id = li.process_id
             left join {{ ref('prep_material_subsets') }} as msub on msub.material_subset_id = li.material_subset_id
             left join {{ source('int_service_supply', 'branded_materials') }} as bmat on bmat.branded_material_id = li.branded_material_id
             left join {{ ref('material_finishes') }} as mf on li.finish_slug = mf.slug
             left join {{ source('int_service_supply', 'material_colors') }} as mc on li.material_color_id = mc.material_color_id -- TODO: does not exist.

            -- Joins for exchange rates
             left join {{ ref('stg_orders_dealstage') }} as order_deals on docs.order_uuid = order_deals.order_uuid
             left join {{ ref('exchange_rate_daily') }} as rates
                             on rates.currency_code_to = docs.currency_code 
                             -- From '2022-04-01' we started using the more appropriate closing date as exchange rate date for closing values instead of quote finalized_at, this has been changed but not retroactively.
                             and trunc(coalesce(case when order_deals.closed_at >= '2022-04-01' then order_deals.closed_at else null end, docs.finalized_at, docs.created)) = trunc(rates.date)

             left join {{ ref('tolerances') }} t on t.id = li.tolerance_id      



    