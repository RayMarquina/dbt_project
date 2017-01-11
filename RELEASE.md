### Release Procedure :shipit:

1. Update changelog
1. Bumpversion
1. Merge to master
  - (on master) git pull origin development
1. Deploy to pypi
  - python setup.py sdist upload -r pypi
1. Deploy to homebrew
  - Make a pull request against homebrew-core
1. Git release notes (points to changelog)
1. Post to slack (point to changelog)
