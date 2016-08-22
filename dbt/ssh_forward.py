
#import sshtunnel
import logging

# modules are only imported once -- make sure that we don't have > 1
# because subsequent tunnels will block waiting to acquire the port

server = None

def get_or_create_tunnel(host, port, user, remote_host, remote_port, timeout):
    pass
    #global server
    #if server is None:
    #    logger = logging.getLogger(__name__)

    #    bind_from = (host, port)
    #    bind_to = (remote_host, remote_port)

    #    # hack
    #    sshtunnel.SSH_TIMEOUT = timeout
    #    server = sshtunnel.SSHTunnelForwarder(bind_from, ssh_username=user, remote_bind_address=bind_to, logger=logger)
    #    try:
    #        server.start()
    #    except sshtunnel.BaseSSHTunnelForwarderError as e:
    #        raise RuntimeError("Problem connecting through {}:{}: {}".format(host, port, str(e)))
    #    except KeyboardInterrupt:
    #        raise RuntimeError('Tunnel aborted (ctrl-c)')

    #return server
