# building models #

### Everything is a `SELECT` ###

The core concept of dbt data models is that everything is a `SELECT` statement. The SQL code within a given model, therefore, defines the dataset, while dbt configuration defines what to do with it.

The advantages of this may not be incredibly clear at first, but here are some things that can be done when thinking about modeling in this way:
- With a single config change, one data model or an entire hierarchy of models can be flipped from views to materialized tables. dbt takes care of wrapping the model's `SELECT` statement in the appropriate `CREATE TABLE` or `CREATE VIEW` syntax.
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
