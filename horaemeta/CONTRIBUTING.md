# Contributing

Thank you for thinking of contributing! We very much welcome contributions from the community.
To make the process easier and more valuable for everyone involved we have a few rules and guidelines to follow.

## Submitting Issues and Feature Requests

Before you file an [issue](https://github.com/apache/incubator-horaedb-meta/issues/new), please search existing issues in case the same or similar issues have already been filed.
If you find an existing open ticket covering your issue then please avoid adding "👍" or "me too" comments; Github notifications can cause a lot of noise for the project maintainers who triage the back-log.
However, if you have a new piece of information for an existing ticket and you think it may help the investigation or resolution, then please do add it as a comment!
You can signal to the team that you're experiencing an existing issue with one of Github's emoji reactions (these are a good way to add "weight" to an issue from a prioritisation perspective).

### Submitting an Issue

The [New Issue]((https://github.com/apache/incubator-horaedb-meta/issues/new)) page has templates for both bug reports and feature requests.
Please fill one of them out!
The issue templates provide details on what information we will find useful to help us fix an issue.
In short though, the more information you can provide us about your environment and what behaviour you're seeing, the easier we can fix the issue.
If you can push a PR with test cases that trigger a defect or bug, even better!

As well as bug reports we also welcome feature requests (there is a dedicated issue template for these).
Typically, the maintainers will periodically review community feature requests and make decisions about if we want to add them.
For features we don't plan to support we will close the feature request ticket (so, again, please check closed tickets for feature requests before submitting them).

## Contributing Changes

Please see the [Style Guide](docs/style_guide.md) for more details.

### Making a PR

To open a PR you will need to have a Github account.
Fork the `horaemeta` repo and work on a branch on your fork.
When you have completed your changes, or you want some incremental feedback make a Pull Request to HoraeDB [here](https://github.com/apache/incubator-horaedb-meta/compare).

If you want to discuss some work in progress then please prefix `[WIP]` to the
PR title.

For PRs that you consider ready for review, verify the following locally before you submit it:

* you have a coherent set of logical commits, with messages conforming to the [Conventional Commits](https://github.com/apache/incubator-horaedb-docs/blob/main/docs/src/en/dev/conventional_commit.md) specification;
* all the tests and/or benchmarks pass, including documentation tests;
* the code is correctly formatted and all linter checks pass; and
* you haven't left any "code cruft" (commented out code blocks etc).

There are some tips on verifying the above in the [next section](#running-tests).

**After** submitting a PR, you should:

* verify that all CI status checks pass and the PR is 💚;
* ask for help on the PR if any of the status checks are 🔴, and you don't know why;
* wait patiently for one of the team to review your PR, which could take a few days.

## Running Tests

The rule for running test has been configured in the [Makefile](./Makefile) so just run:

```shell
make test
```

## Running linter check

CI will check the code formatting and best practices by some specific linters.

And you can run the check locally by the command:

```shell
make check
```
