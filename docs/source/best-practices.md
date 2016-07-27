# best practices #

### Limit dependencies on raw data ###

It's straightforward to make sure that you maintain dependencies within a dbt project using the `ref()` function, but your project will inevitably depend on raw data stored elsewhere in your database. We recommend making what we call "base models" to minimize the dependencies on external tables. The way we have come to use this convention is that base models have the following responsibilities:

- Select only the fields that are important for analysis that is currently ongoing within the project so as to limit complexity. More fields can always be added later.
- Perform any needed type conversion.
- Perform field renaming to rationalize field names into a standard format used within the project.
- **Provide the sole access point to a given raw data table.**

In this convention, all subsequent data models are built on top of base models rather than on top of raw data—only base models are allowed to select from raw data tables. This ensures both that all of the transformations within the base model will be applied to all uses of this data, but also that if the source data table moves (or is located in a different schema or table in a different environment) it can be renamed in a single place.

For a simple example of a base model, check out this (link to a snowplow model).

### Creating trustworthy analytics ###

Software developers often have sophisticated deployment tools for source control and environment management / deployment. Analytics, to-date, has not had the same tooling. Frequently, all analytics is conducted in "production", and ad-hoc mechanisms are used within a given analytics product to know what is trustworthy and what is not. The question "Is this data trustworthy?" can make or break an analytics project, and managing environments and source control are the keys to making sure the answer to that question is always "Yes."

##### Managing multiple environments #####

Currently, dbt supports multiple `run-target`s within a given project. Users can configure a default `run-target` and can override this setting with the `--target` flag passed to `dbt run`. We recommend setting your default run-target to your development environment, and then switch to your production `run-target` on a case-by-case basis.

Using `run-target` to manage multiple environments gives you the flexibility set up your environments how you choose. Commonly, environments are managed by schemas within the same database: all test models are deployed to a schema called `dbt_[username]` and production models are deployed to a schema called `analytics`. An ideal setup would have production and test databases completely separate. Either way, we highly recommend maintaining multiple environments and managing deployments with `run-target`.

##### Source control workflows #####

We believe that all dbt projects should be managed via source control. We use git for all of our source control, and use branching and the pull request process on all multi-user projects. Future versions of dbt will include hooks that will automatically deploy to production upon pushing to master.
