# Testing

**Please note:** testing is still an early feature and not as much work has been put in here yet. Schema testing is very solid, but current testing functionality is, overall, very limited. We anticipate spending much more time working on these features once the core modeling, templating, and scheduling functionality is completed.

## Schema testing

Data integrity in analytic databases is empirically often of lower quality than data in transactional systems. Schema testing provides users a repeatable way to ensure that their schema adheres to basic rules: referential integrity, uniqueness, etc. Building schema tests and running them on an ongoing basis gives users of the resulting data increased confidence that analytic queries produce the desired outputs.

Tests are run with `dbt test`. See [usage](usage/) for more information on the dbt command structure.

Schema tests are declared in a `schema.yml` file that can be placed at any level within your models folders. 
