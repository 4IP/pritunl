from pritunl.constants import *
from pritunl.exceptions import *
from pritunl.helpers import *
from pritunl import settings
from pritunl import logger

import threading
import urllib2
import json
import time

def load_public_ip(attempts=1, timeout=5):
    for i in xrange(attempts):
        if settings.local.public_ip:
            return
        if i:
            time.sleep(3)
            logger.info('Retrying get public ip address', 'setup')
        logger.debug('Getting public ip address', 'setup')
        try:
            request = urllib2.Request(
                settings.app.public_ip_server)
            response = urllib2.urlopen(request, timeout=timeout)
            settings.local.public_ip = json.load(response)['ip']
            break
        except:
            pass
    if not settings.local.public_ip:
        logger.warning('Failed to get public ip address', 'setup')

def setup_public_ip():
    load_public_ip()
    if not settings.local.public_ip:
        thread = threading.Thread(target=load_public_ip,
            kwargs={'attempts': 5})
        thread.daemon = True
        thread.start()
