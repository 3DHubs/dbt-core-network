select orders.uuid                                                                 as  order_uuid,
       orders.created                                                              as  order_created_date,
       orders.promised_shipping_date                                               as  order_promised_shipping_date,
       orders.completed_at                                                         as  order_completed_date,
       orders.expected_shipping_date                                               as  order_expected_ship_date,
       orders.shipped_at                                                           as  order_shipped_date,
       orders.delivered_at                                                         as  order_delivered_date,
       orders.billing_request_id                                                   as  order_billing_request_id,
       orders.cancellation_reason_id                                               as  order_cancellation_reason_id,
       orders.hubspot_deal_id                                                      as  order_hubspot_deal_id,
       orders.hub_id                                                               as  order_hub_id,
       orders.quote_uuid                                                           as  order_quote_uuid,
       orders.is_admin                                                             as  order_is_admin,
       orders.is_automated_shipping_available                                      as  order_is_auto_shipping_available,
       orders.is_strategic                                                         as  order_is_strategic,
       orders.session_id                                                           as  order_session_id,
       orders.status                                                               as  order_status,
       orders.user_id                                                              as  order_user_id,
       orders.legacy_order_id                                                      as  order_legacy_id,
       orders.accepted_at                                                          as  order_accepted_date,
       orders.number                                                               as  document_number,
       quotes.shipping_address_id,
       quotes.document_number                                                      as  order_quote_document_number,
       quotes.currency_code                                                        as  order_currency_code_sold,
       quotes.price_multiplier                                                     as  order_quote_price_multiplier,
       quotes.type                                                                 as  order_quote_type,
       quotes.status                                                               as  order_quote_status,
       quotes.submitted_at                                                         as  order_submitted_date,
       quotes.finalized_at                                                         as  order_quote_finalized_date,
       quotes.shipping_address_id                                                  as  order_quote_shipping_address_id,
       quotes.lead_time                                                            as  order_quote_lead_time,
       quotes.created                                                              as  order_quote_created_date,
       quotes.signed_quote_uuid,
       quotes.tax_category_id,
       quotes.is_cross_docking,
       quotes.cross_docking_added_lead_time,
       quotes.requires_local_production

from {{ ref('cnc_orders') }} as orders

            -- This brings in the "active" quote for an order (subsequent quotes are not included here)
            -- In case of cart quotes, it will be the latest version, in case of submitted or further
            -- down the pipeline, it will be the "locked quote"
            left join {{ ref ('cnc_order_quotes') }} as quotes
                    on quotes.uuid = orders.quote_uuid