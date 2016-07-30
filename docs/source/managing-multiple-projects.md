# Managing multiple projects #

It is common in software engineering to separate useful chunks of code into packages, where the package is responsible for providing some logical unit of functionality. Packages can then be imported into other packages. This modularity is critical to the maintenance of large-scale software engineering efforts.

dbt uses the concept of a package and applies it to analytics. Every dbt project can be imported as a dependency within other projects by providing its git url, and, using the command `dbt deps`, dbt will pull code from this repository and compile it in the same dependency graph as the models within the current project. This is powerful--it allows your models to use `ref()` on top of models that are built and maintained by others.

### dbt and open source ###

Package management is at the heart of the open source community. Open source packages are built and maintained by communities and used by the wider world, and it is the ability to treat code like reusable packages that allows this relationship to thrive.

dbt package management functionality enables the same relationship to exist within analytics. We hope to see particular analytic packages


### Using package management internally ###

(talk about how this is useful internally for multiple discrete projects that may have different authors...)


### Always specify versions ###

(talk about why it's very important to always specify version numbers when pulling in dependencies.)
