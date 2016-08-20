
import psycopg2
import os
import logging

from paramiko import SSHConfig
logging.getLogger("paramiko").setLevel(logging.WARNING)

import dbt.ssh_forward

THREAD_MIN = 1
THREAD_MAX = 8

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

        self.ssh_host = cfg.get('ssh-host', None)
        self.handle = None

    def get_tunnel_config(self):
        config = SSHConfig()

        config_filepath = os.path.join(os.path.expanduser('~'), '.ssh/config')
        config.parse(open(config_filepath))
        options = config.lookup(self.ssh_host)
        return options

    def __open_tunnel(self):
        config = self.get_tunnel_config()
        host = config.get('hostname')
        port = int(config.get('port', '22'))
        user = config.get('user')
        timeout = config.get('connecttimeout', 10)
        timeout = float(timeout)

        if host is None:
            raise RuntimeError("Invalid ssh config for Hostname {} -- missing 'hostname' field".format(self.ssh_host))
        if user is None:
            raise RuntimeError("Invalid ssh config for Hostname {} -- missing 'user' field".format(self.ssh_host))

        # modules are only imported once -- this singleton makes sure we don't try to bind to the host twice (and lock)
        server = dbt.ssh_forward.get_or_create_tunnel(host, port, user, self.host, self.port, timeout)

        # rebind the pg host and port
        self.host = 'localhost'
        self.port = server.local_bind_port

        return server

    def should_open_tunnel(self):
        return self.ssh_host is not None

    # make the user explicitly call this function to enable the ssh tunnel
    # we don't want it to be automatically opened any time someone makes a RedshiftTarget()
    def open_tunnel_if_needed(self):
        self.ssh_tunnel = self.__open_tunnel()

    def cleanup(self):
        if self.ssh_tunnel is not None:
            self.ssh_tunnel.stop()

    def __get_threads(self, cfg):
        supplied = cfg.get('threads', 1)

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
