import argparse
import os.path
import sys
import dbt.project as project
import dbt.task.run as run_task
import dbt.task.compile as compile_task
import dbt.task.debug as debug_task
import dbt.task.clean as clean_task
import dbt.task.deps as deps_task

def main(args=None):
    if args is None:
        args = sys.argv[1:]

    if os.path.isfile('dbt_project.yml'):
        proj = project.read_project('dbt_project.yml')
    else:
        raise RuntimeError("dbt must be run from a project root directory with a dbt_project.yml file")

    p = argparse.ArgumentParser(prog='dbt: data build tool')
    subs = p.add_subparsers()

    base_subparser = argparse.ArgumentParser(add_help=False)
    base_subparser.add_argument('--profile', default=["user"], nargs='+', type=str, help='Which profile to load')

    sub = subs.add_parser('clean', parents=[base_subparser])
    sub.set_defaults(cls=clean_task.CleanTask)

    sub = subs.add_parser('compile', parents=[base_subparser])
    sub.set_defaults(cls=compile_task.CompileTask)

    sub = subs.add_parser('debug', parents=[base_subparser])
    sub.set_defaults(cls=debug_task.DebugTask)

    sub = subs.add_parser('deps', parents=[base_subparser])
    sub.set_defaults(cls=deps_task.DepsTask)

    sub = subs.add_parser('run', parents=[base_subparser])
    sub.set_defaults(cls=run_task.RunTask)

    parsed = p.parse_args(args)

    proj = proj.with_profiles(parsed.profile)
    parsed.cls(args=parsed, project=proj).run()
