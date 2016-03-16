#!/usr/bin/env python
from setuptools import setup, find_packages
import os.path

package_name = "dbt"
package_version = "0.1.0-SNAPSHOT"

setup(
  name=package_name,
  version=package_version,
  packages=find_packages(),
  scripts=[
    'scripts/dbt',
  ],
  install_requires=[
    'argparse>=1.2.1',
    'Jinja2>=2.8',
    'PyYAML>=3.11',
    'psycopg2==2.6.1',
  ],
)
