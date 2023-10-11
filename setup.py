from setuptools import find_packages, setup

setup(name='to-data-library',
      version='1.0',
      packages=find_packages(),
      install_requires=[
          "oauth2client",
          "google-api-python-client",
          "google-cloud-bigquery",
          "google-cloud-storage",
          "paramiko",
          "Jinja2",
          "boto3",
          "pandas",
          "parse",
          "pysftp",
          "delegator.py",
          "markupsafe"])
