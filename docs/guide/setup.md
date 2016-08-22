# Setup

## Installation

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

If you encounter SSL cryptography errors during install on OSX, make sure your copy of pip is up-to-date (via [cryptography.io](https://cryptography.io/en/latest/faq/#compiling-cryptography-on-os-x-produces-a-fatal-error-openssl-aes-h-file-not-found-error)
```bash
› pip install -U pip
› pip install -U dbt
```

## Configuration

To create your first dbt project, run:

```bash
› dbt init [project]
```

This will do two things:
- create a directory at `./[project]` with everything you need to get started.
- create a directory at `~/.dbt/` for environment configuration. [TODO]

Finally, configure your environment:
- supply project configuration within `[project]/dbt_project.yml`
- supply environment configuration within `~/.dbt/profiles.yml`

**Please note: `dbt_project.yml` should be checked in to your models repository, so be sure that it does not contain any database
credentials!** All private credentials belong in `~/.dbt/profiles.yml`.
