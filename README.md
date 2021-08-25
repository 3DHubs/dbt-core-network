# dbt at Hubs

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
- `dbt seed --select seo_page_groups` -- runs only a specific seed
- `dbt seed --full-refresh` -- refreshes the entire seeds.

More info on DBT's documentation:
- [DBT seed](https://docs.getdbt.com/docs/building-a-dbt-project/seeds)
- [DBT seed config](https://docs.getdbt.com/reference/seed-configs)

### `snapshot`
- `dbt snapshot` -- refreshes all snapshots (be careful that no user environments have been set-up yet, so this will run snapshots in PROD)

# Continuous Integration
Once you open a PR, a dbt job will kick-off to validate the proposed changes. This job is currently defined [here](https://cloud.getdbt.com/#/accounts/12103/projects/19451/jobs/26919/). The job exists of three steps:
1.) `dbt seed` -- it (re)creates seed data for all sources in `/data`
2.) `dbt run --models state:modified+ --threads 2 --fail-fast` -- it runs the test only for modified models and their dependencies. Modified means the definition of a model has been changed in comparison to `master`. `--fail-fast` ensures the test fails as fast as possible.
3.) `dbt test --models state:modified+` -- tests are executed on modified models and their dependencies only.

# Sources
Are used to refer to data populated in the data warehouse. Using sources helps us track lineage and allows us to determine source freshness.
- [Introduction to sources](https://docs.getdbt.com/docs/building-a-dbt-project/using-sources).
- More info here on [source properties](https://docs.getdbt.com/reference/resource-properties/freshness).

## Source freshness
Source freshness is configured in [schema.yml](models/schema.yml).

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
- See [dbt style guide](https://github.com/dbt-labs/corp/blob/master/dbt_style_guide.md) for tips how we can organize and format our code.

## Utilities:
- dbt-utils ([source](https://github.com/dbt-labs/dbt-utils))
- redshift ([source](https://github.com/dbt-labs/redshift))

## DB Connection
Currently the Redshift connection is established through a load balancers which is managed in the [EC2 console](https://eu-west-1.console.aws.amazon.com/ec2/v2/home?region=eu-west-1#LoadBalancers:sort=loadBalancerName).

# Actions
- [Stale](https://github.com/actions/stale)

# New dbt developers
Quick checklist on what's required:
- dbt developer seat: ask Data Engineering (Nihad) or director of eng (Paul).
- create Redshift user + password and add user as `dbt_dev` and `ro_group`. The latter is required to get read access on PR-specific dbt schemas in Redshift.

# A note about changing incremental models
In general two types of situations can occur in relation to incremental models.

## Scenario 1: Backfilling source data
In this scenario the incremental model needs to be backfilled with older data. Incremental models typically only look forward (e.g. `where created_at > (select max(created_at) from {{ this }} )`). It may happen that you need to backfill source event data which requires a `dbt run --<your incremental model> -- full-refresh` for the dependent table. Please ensure you isolate the issue and create a separate job to be run in production only for that job.

Example:
```
Before backfill: 
* Raw table A: n=1000, min date = 2020-01-01
* Incremental model X: n=1000, min date = 2020-01-01

After backfill, normal `dbt run`:
* Raw table A: n=2000, min date = 2018-01-01
* Incremental model X: n=1000, min date = 2020-01-01

After backfill, issue `dbt run --models X --full-refresh`
* Raw table A: n=2000, min date = 2018-01-01
* Incremental model X: n=2000, min date = 2018-01-01
```

## Scenario 2: DDL change in source data
In this scenario the source data's schema has changed and you want to have that change propagated to children (i.e. downstream dependencies). Here you'll need to update the projection of that model (`SELECT a, b, <new column>`) in order to make that field available _and_ you will need to issue a full refresh.

```
Before DDL change:
* Raw table A: columns [col_1, col_2, col_3]
* Incrumental model X: columns [col_1, col_2, col_3]

After DDL change, normal `dbt run`
* Raw table A: columns [col_1, col_2, col_4, col_5]
* Incrumental model X: columns [col_1, col_2, col_3]
As you see col_3 is not dropped in X despite the fact it was dropped in A. And it does not break the dbt run command.

After DDL change, issue `dbt run --models X --full-refresh`
* Raw table A: columns [col_1, col_2, col_4, col_5]
* Incrumental model X: columns [col_1, col_2, col_4, col_5]
As you can see col_4 and col_5 are added but col_3 is now missing. If you want to keep col_3 make sure you make a back-up first so you can manually backfill that data later.
```