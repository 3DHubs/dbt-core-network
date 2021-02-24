select id,
       address_id,
       name,
       tax_number,
       created,
       updated,
       deleted,
       decode(suspended, 'false', False, 'true', True)          as is_suspended,
       decode(accepts_auctions, 'false', False, 'true', True)   as is_accepting_auctions,
       currency_code,
       unit_preference,
       tax_number_2,
       decode(send_automatic_rfq, 'false', False, 'true', True) as send_automatic_rfq, -- TODO: better naming convention? {is_,has_}
       decode(allow_for_rfq, 'false', False, 'true', True)      as allow_for_rfq,      -- TODO: better naming convention? {is_,has_}
       default_shipping_carrier_id
from int_service_supply.suppliers