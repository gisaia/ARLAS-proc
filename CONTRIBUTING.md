# Contributing to ARLAS-proc

ARLAS-proc is an open source project and there are many ways to contribute.

## Bug reports

If you think you have found a bug in ARLAS-proc, first make sure it has not already been addressed in our
[issue list](https://github.com/gisaia/ARLAS-proc/issues).

If not, please provide as much information as you can to help us reproduce your bug. Keep in mind that we will fix your problem faster if we can easily reproduce it.

## Feature requests

If you think ARLAS-proc lacks a feature, do not hesitate to open an issue in our
[issue list](https://github.com/gisaia/ARLAS-proc/issues) which describes what you need, why you need it,
and how it should work.

## Contributing code and documentation changes

If you want to submit a bug fix or a feature implementation, first find or open an issue about it in our
[issue list](https://github.com/gisaia/ARLAS-proc/issues)

#### Prerequisites

ARLAS-proc runs with Scala 2.11 (Java 8) and is built/packaged with [sbt](https://www.scala-sbt.org/).

#### Fork and clone the repository

You will need to fork the main ARLAS-proc repository and clone it to your local machine. See
[github help page](https://help.github.com/articles/fork-a-repo) for help.

#### Code formatting

The scala code in ARLAS-proc project is formatted using [Scalafmt](https://scalameta.org/scalafmt/) which easily formats the code `on save`. This tool uses a special configuration file [.scalafmt.conf](.scalafmt.conf)  

#### Submitting your changes

When your code is ready, you will have to:

- rebase your repository.
- run [scripts/tests/test.sh](scripts/tests/test.sh) which should exit with a `0` status.
- update documentation, and tests in [src/test](src/test) if relevant.
- [submit a pull request](https://help.github.com/articles/about-pull-requests/) with a proper title and a reference to the corresponding issue (eg "fix #1234").
- never force push your branch after submitting. If you need to synchronize with the official repository, it is best you merge master into your branch.
