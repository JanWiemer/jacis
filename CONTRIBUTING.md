# Contributing to JACIS

Bug reports, documentation improvements, and pull requests are welcome.

## Before you start

- Open an issue to discuss larger changes before implementing them.
- For security issues, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.

## Build and test

Install JDK 17 or newer. From the repository root, run the Gradle wrapper:

```sh
./gradlew build
```

On Windows, run:

```powershell
.\gradlew.bat build
```

The build compiles the library for Java 11 and runs the test suite. CI also builds and tests on JDK 17, 21, and 25 across Windows, Linux, and macOS.

## Pull requests

- Keep changes focused and follow the existing Java and Gradle conventions.
- Add or update tests when changing behavior.
- Run the full build locally and describe the relevant changes and test results in your pull request.
- Make sure documentation and examples remain consistent with the public API.
