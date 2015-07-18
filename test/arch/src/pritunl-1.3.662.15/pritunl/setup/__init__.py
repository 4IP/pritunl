from pritunl.setup.local import setup_local
from pritunl.setup.app import setup_app
from pritunl.setup.server import setup_server
from pritunl.setup.mongo import setup_mongo
from pritunl.setup.temp_path import setup_temp_path
from pritunl.setup.logger import setup_logger
from pritunl.setup.signal_handler import setup_signal_handler
from pritunl.setup.public_ip import setup_public_ip
from pritunl.setup.poolers import setup_poolers
from pritunl.setup.host import setup_host
from pritunl.setup.host_fix import setup_host_fix
from pritunl.setup.runners import setup_runners
from pritunl.setup.handlers import setup_handlers
from pritunl.setup.check import setup_check
from pritunl.setup.server_cert import setup_server_cert
from pritunl import settings

def setup_all():
    setup_local()
    setup_logger()

    try:
        setup_app()

        if settings.conf.ssl:
            setup_server_cert()

        setup_signal_handler()
        setup_server()
        setup_mongo()
        setup_temp_path()
        setup_public_ip()
        setup_host()
        setup_poolers()
        setup_host_fix()
        setup_runners()
        setup_handlers()
        setup_check()
    except:
        from pritunl import logger
        logger.exception('Pritunl setup failed', 'setup')
        raise

def setup_db():
    setup_local()
    setup_app()

    try:
        if settings.conf.ssl:
            setup_server_cert()

        setup_logger()
        setup_mongo()
    except:
        from pritunl import logger
        logger.exception('Pritunl setup failed', 'setup')
        raise

def setup_loc():
    setup_local()
    setup_app()

    try:
        setup_logger()
    except:
        from pritunl import logger
        logger.exception('Pritunl setup failed', 'setup')
        raise
