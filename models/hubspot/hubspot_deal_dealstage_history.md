# Hubspot Deal Dealstage History
Some note about its data processing and decisions made.

## Functionality
This consists of the static load from Hubspot Dealstage History (from API with history). Code for this can be found [here](https://github.com/3DHubs/lab-analytics/tree/master/ad_hoc/103_hubspot_property_with_version_static_load). This is unioned with the hubspot webhook on dealstage changes to be able to form this complete table with a view through time.

## Known Caveats:
The static dataset generated from dealstage history only goes 25 deep. So if a deal had more dealstage changes than that, it would only have data up until the 25th change.

## To do's
This can be made incremental somehow as it is append only (no backward updates or anything). Only tricky part is the "gluing" together with current table cause the lag needs all history unless you. Do some magic or filter somehow on for instance deal_id present in the landing table for the increment which will work.