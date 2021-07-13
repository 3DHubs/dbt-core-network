# Macros

## `truncate_raw_table`
This macro is designed to remove duplicate rows based on a key and set of columns.

Example use case: Google Ads and Bing Ads keyword peformance reports are synced daily for the last 30 days. That means on day 1 it will fetch performance metrics for day 1. On day 2 it will fetch data for both day 1 and day 2. The original day 1 data stays intact however.

## `varchar_to_boolean`
This macro can be used to cast a boolean field containind text to boolean data type. E.g. 'true' will be cast to True.