# Usage

The following commands are available from the command line to interact with dbt.

## Compile

`dbt compile` generates runnable SQL from model files. All templating is completed, and the dependency graph is built. Resulting SQL files are stored in the `target` directory.

## Run

`dbt run` first compiles, and then runs, model files against the current `run-target` database. dbt connects to the run target and runs the relevant DDL and SQL required to materialize all data models. Models are run in the order defined by the dependency graph generated during compilation. Intelligent multi-threading is used to minimize execution time without violating dependencies.

Deploying new models frequently involves destroying prior versions of these models. In these cases, `dbt run` attempts to minimize the amount of time in which a model is unavailable by first building each model with a temporary name, then dropping the existing model, then renaming the model to its correct name.

## Test

`dbt test` checks the validity of data models. rather than running all models in-place (and potentially dropping existing models), this command deploys all models within a temporary namespace and reports back if there are any errors in deployment. Clean deployment signals that a subsequent `dbt run` in the same environment should be successful.

## Validate

`dbt validate` runs validations on top of deployed models. There are two types of validations:
- schema validations, declared in a `schema.yml` file.
- custom validations, written as SQL `SELECT` statements.

`dbt validate` runs both types of validations and reports the results to the log. Model validation is discussed in more detail here().

## Clean

`dbt clean` is a utility function that deletes all compiled files in the `target` directory.

## Dependencies

`dbt deps` pulls the most recent version of dependencies from git.

## Debug

`dbt debug` is a utility function to show debug information.
