
import psycopg2

class RedshiftTarget:
    def __init__(self, cfg):
        assert cfg['type'] == 'redshift'
        self.host = cfg['host']
        self.user = cfg['user']
        self.password = cfg['pass']
        self.port = cfg['port']
        self.dbname = cfg['dbname']
        self.schema = cfg['schema']

    def __get_spec(self):
        return "dbname='{}' user='{}' host='{}' password='{}' port='{}'".format(
            self.dbname,
            self.user,
            self.host,
            self.password,
            self.port
        )

    def get_handle(self):
        return psycopg2.connect(self.__get_spec())

