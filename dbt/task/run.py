from dbt.runner import Runner

class RunTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        runner = Runner(self.project, self.project['run-target'])
        runner.run()
