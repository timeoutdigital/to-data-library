# to-data-library
Timeout Data Teams Official Data Library

## Python packages

- `setup.py` includes a list of the 3rd party packages required by the this package when distbuted.
- `requirements.in` should include the packages in `setup.py` plus those required for dev/test.
- `requirements.txt` is built from `requirements.in` using `pip-compile`.

## local dev and testing

- Install timeout-tools

```
pip install git+ssh://git@github.com/timeoutdigital/timeout-tools
```

- If you don't have pyenv/pyenv-virtual installed

```
timeout-tools pyenv-install
```

- clone this repo and setup python env

```
timeout-tools ws to-data-library <ticket_num>
```

- populate environment variables # TODO: mock tests so removing requirement for vars

```
export PROJECT=to-data-platform-dev
etc
```

- run unit tests with coverage

```
coverage run -m unittest
```

- view coverage report

```
coverage report
```

- PR tests will fail if coverage is lower that value defined in `devops/pr-buildspec.yml`

```
grep fail-under devops/pr-buildspec.yml
    - coverage report --fail-under=58
```

value should be increased as coverage is improved
