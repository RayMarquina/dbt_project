
class TestTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        print "Running tests!"
