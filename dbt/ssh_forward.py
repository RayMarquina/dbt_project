
from sshtunnel import SSHTunnelForwarder, BaseSSHTunnelForwarderError
import logging

# modules are only imported once -- make sure that we don't have > 1
# because subsequent tunnels will block waiting to acquire the port

server = None

def get_or_create_tunnel(host, port, user, remote_host, remote_port):
    global server
    if server is None:
        logger = logging.getLogger(__name__)

        bind_from = (host, port)
        bind_to = (remote_host, remote_port)

        server = SSHTunnelForwarder(bind_from, ssh_username=user, remote_bind_address=bind_to, logger=logger)
        try:
            server.start()
        except BaseSSHTunnelForwarderError as e:
            raise RuntimeError("Problem connecting through {}:{}: {}".format(host, port, str(e)))

    return server
