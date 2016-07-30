# Overview #

### What is dbt? ###
dbt [data build tool] is a tool for creating analytical data models. dbt facilitates an analytical workflow that closely mirrors software development, including source control, testing, and deployment. dbt makes it possible to produce reliable, modular analytic code as an individual or in teams.

For more information on the thinking that led to dbt, see this article: https://medium.com/analyst-collective/building-a-mature-analytics-workflow-the-analyst-collective-viewpoint-7653473ef05b

### Who should use dbt? ###
dbt is built for data consumers who want to model data in SQL to support production analytics use cases. Familiarity with tools like text editors, git, and the command line is helpful—while you do not need to be an expert with any of these tools, some basic familiarity is important.

### Why do I need to model my data? ###
With the advent of MPP analytic databases like Amazon Redshift and Google BigQuery, it is now common for companies to load and analyze large amounts of raw data in SQL-based environments. Raw data is often not suited for direct analysis and needs to be restructured first. Some common use cases include:
- sessionizing raw web clickstream data
- amortizing multi-month financial transactions

Modeling data transforms raw data into data that can be more easily consumed by business users and BI platforms. It also encodes business rules that can then be relied on by all subsequent analysis, establishing a "single source of truth".

### What exactly is a "data model" in this context? ###
A dbt data model is a SQL `SELECT` statement with templating and dbt-specific extensions.

### How does dbt work? ###

dbt has a small number of core functions. It:
- takes a set of data models and compiles them into raw SQL,
- materializes them into your database as views and tables, and
- runs automated tests on top of them to ensure their integrity.

Once your data models have been materialized into your database, you can write analytic queries on top of them in any SQL-enabled tool.

Conceptually, this is very simple. Practically, dbt solves some big headaches in exactly *how* it accomplishes these tasks:
- dbt interpolates schema and table names in your data models. This allows you to do things like deploy models to test and production environments seamlessly.
- dbt automatically infers a directed acyclic graph of the dependencies between your data models and uses this graph to manage the deployment to your schema. This graph is powerful, and allows for features like partial deployment and safe multi-threading.
- dbt's opinionated design lets you focus on writing your business logic instead of writing configuration and boilerplate code.

### Why model data in SQL? ###

Historically, most analytical data modeling has been done prior to loading data into a SQL-based analytic database. Today, however, it's often preferable to model data within an analytic database using SQL. There are two primary reasons for this:

1. SQL is a very widely-known language for working with data. Providing SQL-based modeling tools gives the largest-possible group of users access.
1. Modern analytic databases are extremely performant, and have sophisticated optimizers. Writing data transformations in SQL allows users to describe transformations on their data but leave the execution plan to the underlying technology. In practice, this provides excellent results with far less work on the part of the author.

Of course, SQL is not a Turing-complete language (to say the least!) and so, will inevitably not be suitable for 100% of potential use cases. dbt may be extended in the future to take advantage of support for non-SQL languages in platforms like Redshift and BigQuery. We have found, though, that modern SQL has a higher degree of coverage than we had originally expected. To users of languages like Python, solving a challenging problem in SQL often requires a different type of thinking, but the advantages of staying "in-database" and allowing the optimizer to work for you are very significant.

### What databases does dbt currently support? ###
Currently, dbt supports PostgreSQL and Amazon Redshift. We anticipate building support for additional databases in the future.
