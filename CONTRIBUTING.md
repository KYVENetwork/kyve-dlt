# Contributing


Thank you for considering to contribute to this project. KYVE DLT is a light-weight
tool for extracting data from the KYVE Datalake (https://www.kyve.network) and
loading it to a supported destination.

## Overview

- The latest state of development is on `main`.
- Releases can be found in `/release/*`.

## Creating a Pull Request

- Check out the latest state from main and always keep the PR in sync with main.
- Use [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/#specification).
- Only one feature per pull request.
- Write an entry for the Changelog.

## Coding Guidelines

- Write readable and maintainable code. `Premature Optimization Is the Root of All Evil`.
  Concentrate on clean interfaces first and only optimize for performance if it is needed.
- The project is structured the following:
  - `/cmd` is the entry point for the program. It contains all commands for the
    CLI.
  - `/destinations` handles the upload and insertion of data rows to a destination.
  - `/loader` manages the entire process including the parallel processing.
  - `/schema` contains the code for obtaining and extracting the data from the
    bundle-format stored on KYVE. It also provides some information to the 
    destination on how to cluster or partition the data.
  - `/utils` contains common functionality used across all modules.

## Legal

You agree that your contribution is licenced under the MIT Licence and all
ownership is handed over to the authors named in [LICENSE](https://github.com/KYVENetwork/chain/blob/main/LICENSE).