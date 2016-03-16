import pprint
import os
import fnmatch
import jinja2


class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def __src_index(self):
        """returns: {'model': ['pardot/model.sql', 'segment/model.sql']}
        """
        indexed_files = {}

        for source_path in self.project['source-paths']:
            for root, dirs, files in os.walk(source_path):
                for filename in files:
                    if fnmatch.fnmatch(filename, "*.sql"):
                        abs_path = os.path.join(root, filename)
                        rel_path = os.path.relpath(abs_path, source_path)
                        indexed_files.setdefault(source_path, []).append(rel_path)

        return indexed_files

    def __write(self, path, payload):
        target_path = os.path.join(self.project['target-path'], path)

        if not os.path.exists(os.path.dirname(target_path)):
            os.makedirs(os.path.dirname(target_path))
        elif os.path.exists(target_path):
            print "Compiler overwrite of {}".format(target_path)

        with open(target_path, 'w') as f:
            f.write(payload)

    def __compile(self, src_index):
        for src_path, files in src_index.iteritems():
            jinja = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath=src_path))
            for f in files:
                template = jinja.get_template(f)
                self.__write(f, template.render(self.project.context()))

    def run(self):
        src_index = self.__src_index()
        self.__compile(src_index)
