# dbt setup #

### installation ###

First, make sure you have postgres installed:

```bash
# linux
sudo apt-get install libpq-dev python-dev

# osx
brew install postgresql
```

Then, install using `pip`:

```bash
› pip install dbt
```

### configuration ###

To create your first dbt project, run:
```bash
› dbt init [project]
```
This will do two things:
- create a [project] directory with everything you need to get started.
- create a directory at `~/.dbt/` for environment configuration.

Finally, configure your environment:
- supply project configuration within `[project]/dbt_project.yml`
- supply environment configuration within `~/.dbt/profiles.yml`


Next: dbt usage
