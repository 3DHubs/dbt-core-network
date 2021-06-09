# DBT @ Hubs

Generally we distinguish three "layers" of data:
(1) landing (or staging) -- the layer that consists of raw data
(2) data_lake -- this layer holds foundational data that has been cleaned, and deduplicated, and enriched
(3) reporting -- this layer holds reporting data (dimensional, factual, and aggregate tables)

## Useful commands and examples

### `run`
- `dbt run` -- refreshes all models
- `dbt run --models tag:finance` -- refreshes all models tagged with finance (tags are cumulative, i.e. dependent models will be refreshed)
- `dbt run --models users+` -- refreshes the `users` model and its dependent models

### `seed`
- `dbt seed` -- loads all seed data

### `snapshot`
- `dbt snapshot` -- refreshes all snapshots (be careful that no user environments have been set-up yet, so this will run snapshots in PROD)

# Sources
Are used to refer to data populated in the data warehouse. Using sources helps us track lineage and allows us to determine source freshness.
- [Introduction to sources](https://docs.getdbt.com/docs/building-a-dbt-project/using-sources).
- More info here on [source properties](https://docs.getdbt.com/reference/resource-properties/freshness).

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
