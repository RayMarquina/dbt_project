
import os, re

def get_version():
    dbt_dir = os.path.dirname(os.path.dirname(__file__))
    version_cfg = os.path.join(dbt_dir, ".bumpversion.cfg")
    if not os.path.exists(version_cfg):
        return "???"
    else:
        with open(version_cfg) as fh:
            contents = fh.read()
            matches = re.search(r"current_version = ([\.0-9]+)", contents)
            if matches is None or len(matches.groups()) != 1:
                return "???"
            else:
                version = matches.groups()[0]
                return version

version = get_version()
