# ORDERS

## Refactor Context

In mid-2021 we did a significant refactor on our legacy tables cube deals and fact deals, in our legacy reporting schema.
The purpose of this was to significantly tidy up the code as it was taking too long to run and it was highly difficult to debug.
We took the old cube deals code and separated it into different models in DBT that would make the logic more structured and understanble.
The table was reduced from 300 columns to 150, ~2300 lines of code to ~1300 split across different models and time to load down from 15 mins to 5 mins.


## New Structure

The new orders table is created in a flow that can be divided into three sections as shown in the figure below. On the first stage we have a collection of staging and aggregate models that query from different sources and cluster data in a logical way based on the source of the data (e.g. agg_order_line_items) or the business application (e.g. stg_orders_finance), these models would be explained in more detail below.

![Screenshot 2021-10-25 at 10 19 57](https://user-images.githubusercontent.com/61149777/138660555-553125f9-43b7-40b0-8317-205a9b7daa87.png)


### Staging

Most tables in the staging stage have either an `stg_` or `agg_` prefix. Table with the STG first prefix collect data from one ore more upstream sources and compile it into a new model, this STG models also serve to limit the number of relevant columns, do name changes, data type transformations and some pre-processing (e.g. currency conversion). The AGG prefix stands for aggregation and collects data from an upstream table that has a many-to-one relationship with the orders table, as an example the model '<agg_orders_line_items>' creates a field '<number_of_line_items>' per order.

Table | Description
------------ | -------------
agg_orders_line_items | Content from cell 2
stg_orders_documents | Content in the second column
stg_orders_logistics | Content in the second column
stg_orders_otr | Content in the second column
stg_orders_dealstage | Content in the second column
stg_orders_finance | Content in the second column
