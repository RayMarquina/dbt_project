
import psycopg2

THREAD_MIN = 1
THREAD_MAX = 10

BAD_THREADS_ERROR = """Invalid value given for "threads" in active run-target.
Value given was {supplied} but it should be an int between {min_val} and {max_val}"""

class RedshiftTarget:
    def __init__(self, cfg):
        assert cfg['type'] == 'redshift'
        self.host = cfg['host']
        self.user = cfg['user']
        self.password = cfg['pass']
        self.port = cfg['port']
        self.dbname = cfg['dbname']
        self.schema = cfg['schema']
        self.threads = self.__get_threads(cfg)

        self.handle = None

    def __get_threads(self, cfg):
        supplied = cfg.get('threads', 1)

        bad_threads_error = RuntimeError(BAD_THREADS_ERROR.format(run_target="...", supplied=supplied, min_val=THREAD_MIN, max_val=THREAD_MAX))

        if type(supplied) != int:
            raise bad_threads_error

        if supplied >= THREAD_MIN and supplied <= THREAD_MAX:
            return supplied
        else:
            raise bad_threads_error

    def __get_spec(self):
        return "dbname='{}' user='{}' host='{}' password='{}' port='{}'".format(
            self.dbname,
            self.user,
            self.host,
            self.password,
            self.port
        )

    def get_handle(self):
        # this is important -- if we use different handles, then redshift
        # fails with a message about concurrent transactions
        if self.handle is None:
            self.handle = psycopg2.connect(self.__get_spec())
        return self.handle

