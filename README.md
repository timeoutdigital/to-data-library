# to-data-library
Timeout Data Teams Official Data Library

## local dev and testing

- Install timeout-tools

```
$ pip install git+ssh://git@github.com/timeoutdigital/timeout-tools
```

- If you don't have pyenv/pyenv-virtual installed

```
$ timeout-tools pyenv-install
```

- setup the python environment
```
$ timeout-tools python-setup
```

- install this repo

```
$ pip install git+https://github.com/timeoutdigital/to-data-library.git@v.1.0.1
```

- populate environment variables # TODO: change to auto populate

```
export PROJECT=to-data-platform-dev
etc
```

- run unit tests

```
python -m unittest
```
