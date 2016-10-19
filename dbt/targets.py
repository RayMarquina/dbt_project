
import psycopg2
import os
import logging

#from paramiko import SSHConfig
#logging.getLogger("paramiko").setLevel(logging.WARNING)
#import dbt.ssh_forward

THREAD_MIN = 1
THREAD_MAX = 8

BAD_THREADS_ERROR = """Invalid value given for "threads" in active run-target.
Value given was {supplied} but it should be an int between {min_val} and {max_val}"""

class BaseSQLTarget:
    def __init__(self, cfg, threads):
        self.target_type = cfg['type']
        self.host = cfg['host']
        self.user = cfg['user']
        self.password = cfg['pass']
        self.port = cfg['port']
        self.dbname = cfg['dbname']
        self.schema = cfg['schema']

        self.threads = self.__get_threads(cfg, threads)

        #self.ssh_host = cfg.get('ssh-host', None)
        self.ssh_host = None
        self.handle = None

    #def get_tunnel_config(self):
    #    config = SSHConfig()

    #    config_filepath = os.path.join(os.path.expanduser('~'), '.ssh/config')
    #    config.parse(open(config_filepath))
    #    options = config.lookup(self.ssh_host)
    #    return options

    #def __open_tunnel(self):
    #    config = self.get_tunnel_config()
    #    host = config.get('hostname')
    #    port = int(config.get('port', '22'))
    #    user = config.get('user')
    #    timeout = config.get('connecttimeout', 10)
    #    timeout = float(timeout)

    #    if host is None:
    #        raise RuntimeError("Invalid ssh config for Hostname {} -- missing 'hostname' field".format(self.ssh_host))
    #    if user is None:
    #        raise RuntimeError("Invalid ssh config for Hostname {} -- missing 'user' field".format(self.ssh_host))

    #    # modules are only imported once -- this singleton makes sure we don't try to bind to the host twice (and lock)
    #    server = dbt.ssh_forward.get_or_create_tunnel(host, port, user, self.host, self.port, timeout)

    #    # rebind the pg host and port
    #    self.host = 'localhost'
    #    self.port = server.local_bind_port

    #    return server

    def should_open_tunnel(self):
        #return self.ssh_host is not None
        return False

    # make the user explicitly call this function to enable the ssh tunnel
    # we don't want it to be automatically opened any time someone makes a new target
    def open_tunnel_if_needed(self):
        #self.ssh_tunnel = self.__open_tunnel()
        pass

    def cleanup(self):
        #if self.ssh_tunnel is not None:
        #    self.ssh_tunnel.stop()
        pass

    def __get_threads(self, cfg, cli_threads=None):
        if cli_threads is None:
            supplied = cfg.get('threads', 1)
        else:
            supplied = cli_threads

        bad_threads_error = RuntimeError(BAD_THREADS_ERROR.format(supplied=supplied, min_val=THREAD_MIN, max_val=THREAD_MAX))

        if type(supplied) != int:
            raise bad_threads_error

        if supplied >= THREAD_MIN and supplied <= THREAD_MAX:
            return supplied
        else:
            raise bad_threads_error

    def __get_spec(self):
        return "dbname='{}' user='{}' host='{}' password='{}' port='{}' connect_timeout=10".format(
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

    def rollback(self):
        if self.handle is not None:
            self.handle.rollback()

    @property
    def type(self):
        return self.target_type

class RedshiftTarget(BaseSQLTarget):
    def __init__(self, cfg, threads):
        super(RedshiftTarget, self).__init__(cfg, threads)


    def sql_columns_in_table(self, schema_name, table_name):
        return """
                select "column" as column_name, "type" as "data_type"
                from pg_table_def
                where schemaname = '{schema_name}' and tablename = '{table_name}'
               """.format(schema_name=schema_name, table_name=table_name).strip()

    @property
    def context(self):
        return {
            "sql_now": "getdate()"
        }

class PostgresTarget(BaseSQLTarget):
    def __init__(self, cfg, threads):
        super(PostgresTarget, self).__init__(cfg, threads)

    def sql_columns_in_table(self, schema_name, table_name):
        return """
                select column_name, data_type
                from information_schema.columns
                where table_schema = '{schema_name}' and table_name = '{table_name}'
               """.format(schema_name=schema_name, table_name=table_name).strip()

    @property
    def context(self):
        return {
            "sql_now": "clock_timestamp()"
        }

target_map = {
    'postgres': PostgresTarget,
    'redshift': RedshiftTarget
}

def get_target(cfg, threads=1):
    target_type = cfg['type']
    if target_type in target_map:
        klass = target_map[target_type]
        return klass(cfg, threads)
    else:
        valid_csv = ", ".join(["'{}'".format(t) for t in target_map])
        raise RuntimeError("Invalid target type provided: '{}'. Must be one of {}".format(target_type, valid_csv))
