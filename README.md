### dbt

A data build tool

#### installation

```bash
› pip install dbt
```

#### configuration

  Create a `dbt_project.yml` file in the root of your [models](https://github.com/analyst-collective/models) directory. Also create a 
  `profiles.yml` file in the `~/.dbt` director. If this directory doesn't exist, you should create it. The
  `dbt_project.yml` file should be checked in to your models repository, so it should *not* contain any database
  credentials. Make sure that private information is stored in the `~/.dbt/profiles.yml` configuration file!

```yml
# dbt_project.yml
test-paths: ["test"]
source-paths: ["model"]   # paths with source code to compile
target-path: "target"     # path for compiled code
clean-targets: ["target"] # directories removed by the clean task

# ~/.dbt/profiles.yml
user:
  outputs:
    my-redshift: # uniquely named
      type: redshift # only type supported
      host: localhost # any IP or fqdn
      port: 5439
      user: my_user
      pass: password
      dbname: dev
      schema: my_model_schema
  run-target: my-redshift
```

#### use

`dbt compile` to generate runnable SQL from model files

`dbt run` to run model files on the current `run-target` database

`dbt clean` to clear compiled files

#### troubleshooting




#### contributing

From the root directory of this repository, run:
```bash
› python setup.py develop
```

to install a development version of `dbt`.
