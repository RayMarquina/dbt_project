### dbt

A data build tool

#### installation

From the root directory of this repository,

```bash
› python setup.py develop
```

#### use

Create a `dbt_project.yml` file in the root of your source tree
following sample.dbt_project.yml. If you place the config file
in your root directory (~/.dbt/profiles.yml), be sure to subclass
your configuration paramters under `user`, eg:

```yml
user:
  source-paths: [...]
  ...
  outputs:
    ...
```

`dbt compile` to generate runnable SQL from model files

`dbt run` to run model files on the current `run-target` database

`dbt clean` to clear compiled files
