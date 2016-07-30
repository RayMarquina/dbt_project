# Best practices #

### Limit dependencies on raw data ###

It's straightforward to make sure that you maintain dependencies within a dbt project using the `ref()` function, but your project will inevitably depend on raw data stored elsewhere in your database. We recommend making what we call "base models" to minimize the dependencies on external tables. The way we have come to use this convention is that base models have the following responsibilities:

- Select only the fields that are relevant for current analytics to limit complexity. More fields can always be added later.
- Perform any needed type conversion.
- Perform field renaming to rationalize field names into a standard format used within the project.
- **Act as the sole access point to a given raw data table.**

In this convention, all subsequent data models are built on top of base models rather than on top of raw data—only base models are allowed to select from raw data tables. This ensures both that all of the transformations within the base model will be applied to all uses of this data and that if the source data table moves (or is located in a different schema or table in a different environment) it can be renamed in a single place.

For a simple example of a base model, check out this (link to a snowplow model).

### Creating trustworthy analytics ###

Software developers often use sophisticated tools for source control, environment management, and deployment. Analytics, to-date, has not had the same tooling. Frequently, all analytics is conducted in "production", and ad-hoc mechanisms are used within a given analytics product to know what is trustworthy and what is not. The question "Is this data trustworthy?" can make or break an analytics project, and managing environments and source control are the keys to making sure the answer to that question is always "Yes."

#### Managing multiple environments ####

Currently, dbt supports multiple `run-target`s within a given project within `~/.dbt/profiles.yml`. Users can configure a default `run-target` and can override this setting with the `--target` flag passed to `dbt run`. We recommend setting your default `run-target` to your development environment, and then switch to your production `run-target` on a case-by-case basis.

Using `run-target` to manage multiple environments gives you the flexibility set up your environments how you choose. Commonly, environments are managed by schemas within the same database: all test models are deployed to a schema called `dbt_[username]` and production models are deployed to a schema called `analytics`. An ideal setup would have production and test databases completely separate. Either way, we highly recommend maintaining multiple environments and managing deployments with `run-target`.

#### Source control workflows ####

We believe that all dbt projects should be managed via source control. We use git for all of our source control, and use branching and pull requests to keep the master branch the sole source of organizational truth. Future versions of dbt will include hooks that will automatically deploy to production upon pushing to master.

#### Using dbt interactively ####

The best development tools allow for very small units of work to be developed and tested quickly. One of the major advantages of dbt is getting analytics out of clunky tools and into text files that can be edited in whatever your editor of choice is—we have folks using vim, emacs, and Atom.

When your project gets large enough, `dbt run` can begin to take a while. This stage in your development could be a bottleneck and slow you down. dbt provides three primary ways to address this:

1. Use views instead of tables to the greatest extent possible in development. Views typically deploy much faster than tables, and in development it's often not critical that subsequent analytic queries run as fast as possible. It's easy to change this setting later and it will have no impact on your business logic.
1. Use `dbt_project.yml` to disable portions of your project that you're not currently working on. If you have multiple modules within a given project, turn off the ones that you're not currently working on so that those models don't deploy with every `dbt run`.
1. Pass the `--model` flag to `dbt run`. This flag asks dbt to only `run` the models you specify and their dependents. If you're working on a particular model, this can make a very significant difference in your workflow.
