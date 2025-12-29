# Contributing to Jocko

## Getting Started

When creating a pull request you should:

- **Open an issue first**: 
    - Confirm that the change will be accepted
    - Describe what your change does and why it's useful
    - Link relevant Kafka documentation or code if applicable

- **Fork and clone the repo**:
    ```bash
    git clone git@github.com:your-username/jocko.git
    cd jocko
    ```

- **Install dependencies**:
    ```bash
    go mod download
    ```

- **Check the tests pass**:
    ```bash
    go test ./...
    ```

- **Run the linter**:
    ```bash
    golangci-lint run
    ```

## Making Changes

1. Create a feature branch from `master`
2. Make your changes
3. Write tests for new functionality
4. Ensure all tests pass
5. Ensure linting passes
6. Commit with a clear message

## Commit Messages

- Start with a lowercase verb: "add", "fix", "refactor", "remove"
- Reference issues by including "closes #N" or "fixes #N"
- Keep the first line under 72 characters

## Code Style

- Run `gofmt` and `goimports` on your code
- Follow existing patterns in the codebase
- Add comments for exported functions and types

Thanks for contributing!
