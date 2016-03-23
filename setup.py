#!/usr/bin/env python
from setuptools import setup, find_packages
import os.path

package_name = "dbt"
package_version = "0.1.1"

setup(
  name=package_name,
  version=package_version,
  description="Data build tool for Analyst Collective",
  author="Analyst Collective",
  author_email="admin@analystcollective.org",
  url="https://github.com/analyst-collective/dbt",
  packages=find_packages(),
  scripts=[
    'scripts/dbt',
  ],
  install_requires=[
    #'argparse>=1.2.1',
    'Jinja2>=2.8',
    'PyYAML>=3.11',
    'psycopg2==2.6.1',
  ],
)
