# Contributing to Flask Async Celery

Thanks for your interest in contributing to `flask-async-celery`! Contributions of all sizes are welcome, including bug reports, documentation improvements, tests, and code changes.

This guide explains how to get started and submit changes.

## Getting Started

### 1. Fork and clone the repository

Fork the repository on GitHub, then clone your fork:

```bash
git clone https://github.com/YOUR_USERNAME/flask_celery_async.git
cd flask_celery_async
```

Add the original repository as an upstream remote if you want to keep your fork synchronized:

```bash
git remote add upstream https://github.com/mazharalidev1/flask_celery_async.git
```

If `upstream` already exists, you can skip that command.

### 2. Set up the development environment

Use a supported Python version for the project.

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

Install the project and its development dependencies using the instructions in `pyproject.toml` and the README.

If the project defines a development extra, you can install it with:

```bash
pip install -e ".[dev]"
```

If that extra is not defined, install the dependencies using the project's documented development setup instead.

### 3. Run the tests

Before making changes, run the test suite:

```bash
python -m pytest
```

Some integration tests may require services such as Redis. Check the README and test configuration for any additional setup requirements.

Please make sure your changes do not break existing tests.

## Making Changes

### Branches

Create a separate branch for each change:

```bash
git switch -c fix/short-description
```

Use a descriptive branch name, such as:

- `fix/retry-tracing`
- `docs/improve-quickstart`
- `test/worker-shutdown`
- `feat/add-monitoring-option`

### Code Guidelines

When contributing code:

- Follow the existing project structure and coding style.
- Keep changes focused and avoid unrelated refactoring.
- Add or update tests for bug fixes and new behavior.
- Update documentation when changing public APIs, configuration, or behavior.
- Preserve backward compatibility unless a breaking change is intentional and documented.
- Avoid adding dependencies unless they are necessary.
- Handle exceptions, task retries, cancellation, and worker shutdown carefully.
- Keep optional integrations optional where the project design requires it.

For changes involving asynchronous task execution, consider how they behave under concurrency, during retries, and when a worker shuts down.

For OpenTelemetry changes, include tests for tracing behavior where practical.

## Testing Your Changes

Run the relevant tests while developing:

```bash
python -m pytest
```

Before submitting a pull request, also check the diff:

```bash
git diff --check
git status --short
```

If your change affects packaging, verify that the package can still be built using the build instructions documented by the project.

If a test requires an external service that you cannot run locally, mention that limitation in your pull request.

## Commit Messages

Write short, descriptive commit messages that explain the change.

Examples:

```text
Fix retry span status handling
Add worker shutdown tests
Improve OpenTelemetry setup documentation
Update monitoring example
```

Avoid combining unrelated changes into one commit.

## Submitting a Pull Request

1. Commit your changes on your branch.
2. Push the branch to your fork.
3. Open a pull request against the repository's default branch.
4. Explain the problem your change solves and why the change is needed.
5. Include relevant test results.
6. Link related issues, if applicable.

A useful pull request description includes:

- **Summary:** What changed?
- **Reason:** Why is the change needed?
- **Testing:** What did you run?
- **Compatibility:** Does the change affect existing users?

Please keep pull requests focused and reasonably small so they are easier to review.

## Reporting Bugs

Before opening a bug report, check whether the issue has already been reported.

When submitting a bug report, include:

- Python and package versions
- Operating system and relevant service versions
- A minimal example that reproduces the issue
- The expected behavior and actual behavior
- Relevant error messages or logs, with secrets and sensitive information removed

For security vulnerabilities, do not open a public issue. Use GitHub's private vulnerability reporting feature if it is enabled for the repository.

## Suggesting Features

Feature requests are welcome. Explain the problem you want to solve, the expected behavior, and any alternatives you have considered.

For substantial changes, open an issue for discussion before spending time on a large implementation.

## Documentation

Documentation improvements are contributions too.

Please keep examples consistent with the current API and test code snippets when practical. If a change affects installation, configuration, task execution, retries, monitoring, or tracing, update the relevant documentation.

## Code of Conduct

By participating in this project, you agree to follow the guidelines in [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Questions

If you're unsure whether a change fits the project, open a GitHub issue or discussion before starting significant work.

Thanks for helping improve Flask Async Celery!
