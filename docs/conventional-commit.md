# Conventional Commit Guide

This document describes how we use [conventional commit](https://www.conventionalcommits.org/en/v1.0.0/) in our development.

# Structure

We would like to structure our commit message like this:
```
<type>[optional scope]: <description>
```

There are three parts. `type` is used to classify which kind of work this commit do. `scope` is a optional field that provides additional contextual information. And the last field is your `description` to this commit.

# Type

Here we list some common `type`s and their meanings.

- `feat`: Implement a new feature.
- `docs`: Add document or comment.
- `build`: Change the build script or configuration.
- `style`: Style change (only). No logic involved.
- `refactor`: Refactor a existing module for performance, structure or other reason.
- `test`: Enhance test coverage or harness.
- `chore`: None of the aboves.

# Scope

The `scope` is more flexible than `type`. And it may have different values under different `type`s.

For example, In a `feat` or `build` commit we may use the code structure to define scope, like

```
feat(cluster): 
feat(server):

build(ci):
build(image):
```

And in `docs` or `refactor` commits the motivation is prefer to label the `scope`, like

```
docs(comment):
docs(post):

refactor(perf):
refactor(usability):
```

But you don't need to add a scope every times. This isn't mandatory. It's just a way to help describe the commit.

# After all

There are many other rules or scenarios in [conventional commit](https://www.conventionalcommits.org/en/v1.0.0/)'s website. We are still exploring for a better and more friendly workflow. Please do let us know if you have any suggestion and/or suggestion by [open a issue](https://github.com/CeresDB/ceresdb/issues/new/choose) ❤️


