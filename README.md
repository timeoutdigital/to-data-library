

# to-data-library

to-data-library is a Python library for data extraction, transformation, and loading (ETL) across multiple platforms (GCS, S3, BigQuery, FTP, etc). It is intended to be imported and used as a module within other data engineering projects, scripts, or pipelines. This is not a standalone application and is not designed to be run directly, in Docker, or via Airflow on its own.

---


## Features

- Extracts and loads data from Google Cloud Storage, S3, BigQuery, FTP, and more.
- Provides transformation utilities for pandas DataFrames.
- Automated dependency management and code linting.

---


## Project Structure

```
to-data-library/
├── to_data_library/
│   ├── data/                  # Core data transfer logic (GCS, S3, BQ, FTP)
│   ├── __init__.py
├── tests/                    # Unit tests and test data
├── devops/                   # CI/CD buildspecs
├── requirements.in           # Python dependencies (source)
├── requirements.txt          # Python dependencies (compiled)
├── setup.py                  # Python package setup
├── PYTHON_VERSION            # Python version pin
└── README.md
```

---


## Local Development


### Prerequisites

- Python 3.10.x (see `PYTHON_VERSION`)
- [timeout-tools](https://github.com/timeoutdigital/timeout-tools) for environment setup


### Setup

```sh
# Install timeout-tools
pip install git+ssh://git@github.com/timeoutdigital/timeout-tools

# Clone the repo and set up Python environment
git clone git@github.com:timeoutdigital/to-data-library.git
cd to-data-library
timeout-tools python-setup
```

Or, using workspace setup:

```sh
timeout-tools ws to-data-library <jira_ticket>
```

- setup.py includes a list of the 3rd party packages required by the this package when distributed.

### Install dependencies

```sh
invoke python-install-requirements
```


### Running Unit Tests

```sh
coverage run -m unittest
coverage report
```

PR tests will fail if coverage is lower than the value defined in `devops/pr-buildspec.yml`:

```sh
grep fail-under devops/pr-buildspec.yml
# Example: coverage report --fail-under=78
```

Increase the value as coverage improves.

You can also use:
```sh
pytest
```

---


## Usage

Import `to_data_library` in your own Python scripts or projects:

```python
from to_data_library.data import transfer

client = transfer.Client(project='my-gcp-project')
# Use client methods for data transfer, e.g. client.gs_to_bq(...)
```

This library is not intended to be run directly or as a standalone service. It does not provide a CLI or entrypoint script.

---



## AWS & GCP Resource Access

- The Data Apps run as is allowed to use specific prod/staging AWS IAM Roles.
- These AWS IAM Roles are defined in:
    - https://github.com/timeoutdigital/platform/blob/master/cloudformation/data-app-base.yml

---


## CI/CD

- Build and linting are handled via AWS CodeBuild using buildspecs in [`devops/`](devops/).
- Pre-commit hooks are configured in [`.pre-commit-config.yaml`](.pre-commit-config.yaml) if present.

---


## Useful Commands

- Compile requirements:
    `invoke python-build-requirements`
- Upgrade requirements:
    `invoke python-upgrade-requirements`
- Install requirements:
    `invoke python-install-requirements`
- Run pre-commit hooks:
    `pre-commit run --all-files`

---


## License

_Proprietary - Timeout.com_

---


## Contact

For questions, contact the Data Engineering team at Timeout.com.
