# building models #

### Everything is a `SELECT` ###

The core concept of dbt data models is that everything is a `SELECT` statement. Using this approach, the SQL code within a given model defines the dataset, while dbt configuration defines what to do with it.

The advantages of this may not be incredibly clear at first, but here are some things that can be done when thinking about specifying data models this way:
- With a single config change, one data model or an entire hierarchy of models can be flipped from views to materialized tables. dbt takes care of wrapping a model's `SELECT` statement in the appropriate `CREATE TABLE` or `CREATE VIEW` syntax.
- With two configuration changes, a model can be flipped from a materialized table that is rebuilt with every `dbt run` to a table that is built incrementally, inserting the most recent rows since the most recent `dbt run`. dbt will wrap the select into an `INSERT` statement and automatically generate the appropriate `WHERE` clause.
- With one config change, a model can be made ephemeral. Instead of being deployed into the database, ephemeral models are pulled into dependent models as common table expressions.

Because every model is a `SELECT`, these behaviors can all be configured very simply, allowing for flexibility in development workflow and production deployment.

### Using `ref()` ###

dbt models support interpolation via the Jinja2 templating language. This presents many powerful options for building data models, many of which are only now beginning to be explored! The most important function in dbt is `ref()`; it's impossible to build even moderately complex models without it.

`ref()` is how you reference one model within another. This is a very common behavior, as typically models are built to be "stacked" on top of one another to create increasing analytical sophistication. Here is how this looks in practice:

```sql
--filename: model_a.sql

select *
from public.raw_data
```
```sql
--filename: model_b.sql

select *
from {{ref('model_a')}}
```

`ref()` is, under the hood, actually doing two important things. First, it is interpolating the schema into your model file to allow you to change your deployment schema via configuration. Second, it is using these references between models to automatically build the dependency graph. This will enable dbt to deploy models in the correct order when using `dbt run`.

When calling to Jinja2, functions are wrapped in double brackets—`{{}}`)—so writing `ref('model_name')` must actually be done as `{{ref('model_name')}}`

### Model configuration ###

There are a number of configuration options provided to control how dbt interacts with your models. Understanding these configuration options is core to controlling dbt's behavior and optimizing its usage.

#### Supplying configuration values ####

There are multiple ways to supply model configuration values:

- as model defaults that apply to an entire project
- as settings for an entire group of modules, applied at the directory level
- as settings for an individual model, specified within a given model.

All methods accept identical configuration options. dbt provides multiple configuration contexts in order to give model authors maximum control over their model behaviors. In all cases, configuration provided at a more detailed level overrides configuration provided at a more generic level.

Here is how these configuration options look in practice:

```YAML
# dbt_project.yml

model-defaults: # specify configuration for all models in a project
  enabled: true # other values : false
  materialized: table # other values: view, incremental, ephemeral

models:
  [model-group]: # model groups can be arbitrarily nested and reflect the directory structure of your project.
    [settings]: # the same settings (e.g. enabled, materialized) can be applied to model groups as could be specified in model-defaults
```

(Specify model configuration within models, but need to wait until this code is updated to document it.)

#### Using `materialized` ####

The `materialized` option is provided like so:

```YAML
  materialized: table # other values: view, incremental, ephemeral
```

Each of the four values passed to `materialized` significantly changes how dbt builds the associated models:

- `table` wraps the `SELECT` in a `CREATE TABLE AS...` statement. This is a good option for long-running queries, but requires the model to be re-built in order to get new data.
- `view` wraps the `SELECT` in a `CREATE VIEW AS...` statement. This is a good option for snappy queries, as it ensures that data selected from this model is always up-to-date with no table re-builds required.
- `incremental` performs two functions: 1) if the model does not exist in the database, dbt will create it and populate it with the results of the `SELECT`. If the model already exists, dbt will wrap the `SELECT` within an `INSERT` statement and apply a where clause as defined by the `incremental-id` parameter. This is a powerful option for large, immutable tables like log files and clickstream data. For more information, read the section on incremental models below.
- `ephemeral` prevents dbt from materializing the model directly into the database. Instead, dbt will interpolate the code from this model into dependent models as one or more common table expressions. This is useful if you want to write reusable logic but don't feel that data consumers should be selecting directly from this model.

#### Using `enabled` ####

This parameter does exactly what you might think. Setting `enabled` to `false` tells dbt not to compile and run the associated models. Be careful disabling large swaths of your project: if you disable models that are upstream of enabled models in the dependency chain, compilation will fail. Instead, use `enabled` to turn off sections of your project that are unrelated to the models you want to deploy.

Note that dbt does not actively delete models in your database that have been disabled. Instead, it simply leaves them out of future rounds of compilation and deployment. If you want to delete models from your schema, you will have to perform this by hand.

#### Using `sortkey` and `distkey` ####

Tables in Amazon Redshift have two powerful optimizations to improve query performance: distkeys and sortkeys. Supplying these values as model-level configurations apply the corresponding settings in the generated `CREATE TABLE` DDL. Note that these settings will have no effect for models set to `view` or `ephemeral`.

For more information on distkeys and sortkeys, view Amazon's docs.

### Incremental models ###

Incremental models are a powerful feature in production dbt deployments. Frequently, certain raw data tables can have billions (or more) rows, which makes performing frequent rebuilds of models dependent on these tables impractical. Incremental tables provide another option. The first time a model is deployed, data is populated in a `CREATE TABLE AS SELECT...` statement, but in subsequent runs this model will have incremental rows populated via an `INSERT` statement wrapped around the underlying `SELECT`.

The key to making incremental models work is supplying a `WHERE` condition that identifies the rows that need to be added. For instance, in a clickstream table, you might apply the condition `WHERE [source].event_date > [model].event_date`. Practically, dbt applies this where condition for you, you just need to specify the field name to apply the condition on via the `incremental-id` parameter. All incremental tables require this parameter.

It's highly recommended to use incremental models rather than basic tables in production whenever the schema allows for it. This will minimize your model build times and minimize the use of database resources.
