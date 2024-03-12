# Macros

## `grant_select_on_schemas`
This macro is referred to by [dbt_project.yml](../dbt_project.yml) to manage access to dbt-related schemas to Redshift users. The function requires two arguments:
* schemas
* group

In dbt_project.yml we feed it with `schemas` which will actually feed it an array of all custom schemas that are relevant to the jobs run. E.g. if dbt is executed in prod it will include `int_analytics`, `reporting`, etc. See [schemas](https://docs.getdbt.com/reference/dbt-jinja-functions/schemas) documentation for more information.

Secondly we pass on the `ro_group` as second argument. Each Redshift user with this role will be granted read access to all dbt models. Maybe this is not desired in the future because those users will have read access to seed data as well as access to int_analytics tables. That's up to the future dbt masters. 😄

Zum schluss. This macro is executed at the end of each step, thus after `dbt seed`, `dbt snapshot`, `dbt run`, etc. See [dbt's documentation](https://docs.getdbt.com/reference/project-configs/on-run-start-on-run-end) for more info on this feature.


## `truncate_raw_table`
This macro is designed to remove duplicate rows based on a key and set of columns.

Example use case: Google Ads and Bing Ads keyword peformance reports are synced daily for the last 30 days. That means on day 1 it will fetch performance metrics for day 1. On day 2 it will fetch data for both day 1 and day 2. The original day 1 data stays intact however.

## `varchar_to_boolean`
This macro can be used to cast a boolean field containind text to boolean data type. E.g. 'true' will be cast to True.
