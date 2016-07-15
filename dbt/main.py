import argparse
import os.path
import sys
import dbt.project as project
import dbt.task.run as run_task
import dbt.task.compile as compile_task
import dbt.task.debug as debug_task
import dbt.task.clean as clean_task
import dbt.task.deps as deps_task
import dbt.task.init as init_task
import dbt.task.seed as seed_task
import dbt.task.test as test_task

def main(args=None):
    if args is None:
        args = sys.argv[1:]

    p = argparse.ArgumentParser(prog='dbt: data build tool')
    subs = p.add_subparsers()

    base_subparser = argparse.ArgumentParser(add_help=False)
    base_subparser.add_argument('--profile', default=["user"], nargs='+', type=str, help='Which profile to load')
    base_subparser.add_argument('--target', default=None, type=str, help='Which run-target to load for the given profile')

    sub = subs.add_parser('init', parents=[base_subparser])
    sub.add_argument('project_name', type=str, help='Name of the new project')
    sub.set_defaults(cls=init_task.InitTask, which='init')

    sub = subs.add_parser('clean', parents=[base_subparser])
    sub.set_defaults(cls=clean_task.CleanTask, which='clean')

    sub = subs.add_parser('compile', parents=[base_subparser])
    sub.set_defaults(cls=compile_task.CompileTask, which='compile')

    sub = subs.add_parser('debug', parents=[base_subparser])
    sub.set_defaults(cls=debug_task.DebugTask, which='debug')

    sub = subs.add_parser('deps', parents=[base_subparser])
    sub.set_defaults(cls=deps_task.DepsTask, which='deps')

    sub = subs.add_parser('run', parents=[base_subparser])
    sub.set_defaults(cls=run_task.RunTask, which='run')

    sub = subs.add_parser('seed', parents=[base_subparser])
    sub.add_argument('--drop-existing', action='store_true', help="Drop existing seed tables and recreate them")
    sub.set_defaults(cls=seed_task.SeedTask, which='seed')

    sub = subs.add_parser('test', parents=[base_subparser])
    sub.add_argument('--skip-test-creates', action='store_true', help="Don't create temporary views to validate model SQL")
    sub.add_argument('--validate', action='store_true', help='Run constraint validations from schema.yml files')
    sub.set_defaults(cls=test_task.TestTask, which='test')

    if len(args) == 0: return p.print_help()

    parsed = p.parse_args(args)

    if parsed.which == 'init':
        # bypass looking for a project file if we're running `dbt init`
        parsed.cls(args=parsed).run()

    elif os.path.isfile('dbt_project.yml'):
        try:
          proj = project.read_project('dbt_project.yml', validate=False).with_profiles(parsed.profile)
          proj.validate()
        except project.DbtProjectError as e:
          print("Encountered an error while reading the project:")
          print("  ERROR {}".format(e.message))
          print("Did you set the correct --profile? Using: {}".format(parsed.profile))
          all_profiles = project.read_profiles().keys()
          profiles_string = "\n".join([" - " + key for key in all_profiles])
          print("Valid profiles:\n{}".format(profiles_string))
          return

        if parsed.target is not None:
          targets = proj.cfg.get('outputs', {}).keys()
          if parsed.target in targets:
            proj.cfg['run-target'] = parsed.target
          else:
            print("Encountered an error while reading the project:")
            print("  ERROR Specified target {} is not a valid option for profile {}".format(parsed.target, parsed.profile))
            print("Valid targets are: {}".format(targets))
            return

        parsed.cls(args=parsed, project=proj).run()

    else:
        raise RuntimeError("dbt must be run from a project root directory with a dbt_project.yml file")


