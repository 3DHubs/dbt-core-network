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

Most tables in the staging stage have either an `stg_` or `agg_` prefix. Table with the `stg` prefix collect data from one or more upstream sources and compile it into a new model, this stg models also serve to limit the number of relevant columns, do name changes, data type transformations and some pre-processing (e.g. currency conversion). The `agg` prefix stands for aggregation and collects data from an upstream table that has a many-to-one relationship with the orders table, as an example the model `agg_orders_line_items` creates a field `number_of_line_items` per order.

Table | Description
------------ | -------------
stg_orders_hubspot | Queries from data_lake `hubspot_deals` and some data from `hubspot_owners`. Although the construction of this table is rather simple is very important for the fact_orders table as several fields are defined from this source, Hubspot is the CRM software used primarily by the Sales team but also widely use across Hubs to keep track of the status of an order. It also provides us with the contact and company Hubspot IDs that allows us to join the orders table with the contact and company tables.
stg_orders_documents | Takes data from `dbt_prod_data_lake.cnc_order_quotes` which contains data from quotes and purchase orders (POs). Although labelled as a staging table it also aggregates data as one order can have multiple quotes and POs. Quotes and POs contain valuable data such as amounts, timestamps, document numbers and more.
stg_orders_otr | OTR stands for On Time Rate. This is an example of a table that is built on top of two other staging tables. It compares the data from `stg_orders_logistcs` and `stg_orders_documents` to determine if an order was on time or not, the logistics model determines what actually happened whereas the documents model states what was agreed/promised.
stg_orders_dealstage | Takes data from `hubspot_deal_dealstage_history` and `fact_order_events`, the later table is a filtered version of `order_history_events`. This table uses this data to determine the status of the order at certain stages, the most critical event defined here is `closing`.
agg_orders_line_items | Example of an aggregated table. Takes data from both `data_lake.line_items` and `dbt_prod_reporting.fact_line_items` and creates aggregate fields such as number_of_line_items, has_custom_material, etc. 

Not all models are described in the above table but hopefully this helps as an overview of how the staging section is designed and the differences between staging and aggregated tables. For further details on other models please refer to their code, a brief explanation should be provided on each.

### Compilation

The model `stg_fact_orders` is where all staging and aggregated models are compiled together with the main `cnc_orders` table coming from our Supply database. In theory little processing is done in this model and should be placed in the staging models. Nevertheless in some situation this is unavoidable as different staging models need to be compared for the definition of a field or some small processing is required. As mentioned the base table for this model come from the `cnc_orders` table and hence it includes all orders including not-submitted ones (a.k.a orders in cart status). There are two differences: empty carts are filtered out as well as legacy orders (appended later).


### Post-Processing

Not all processing can be done at the compilation and unavoidably there is a need for an extra step after the compilation of the staging tables. The main table `fact_orders` queries from `stg_fact_orders` with some differences:

(1) **Legacy Data:** A static legacy order data living in data_lake schema is unioned to the model (not visible in the diagram).
(2) **Order Aggregates**: some fields such as number_of_orders_contact, became_customer_at_contact are created in the `agg_orders` model and then joined on fact_orders. This model is also later joined to the companies and contacts table (not shown on the diagram).
(3) **Contribution Margin:** CM1 needs to be defined at this stage as an invoice is recognized based on the recognition date of the order which is defined in the stg_fact_orders model.
(4) **Re-Orders**: Some orders might have a re-order and this would then have a original_order_uuid, to determine the characteristics of the original order a self-join is required which needs to be done at this stage.
