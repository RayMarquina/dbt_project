from dbt.compilation import Compiler, CompilableEntities
from dbt.templates import BaseCreateTemplate
from dbt.logger import GLOBAL_LOGGER as logger


class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        compiler = Compiler(self.project, BaseCreateTemplate, self.args)
        compiler.initialize()
        results = compiler.compile(limit_to=CompilableEntities)

        stat_line = ", ".join(
            ["{} {}".format(results[k], k) for k in CompilableEntities]
        )
        logger.info("Compiled {}".format(stat_line))
