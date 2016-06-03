from pritunl.exceptions import *
from pritunl.helpers import *
from pritunl import logger
from pritunl import settings
from pritunl import wsgiserver
from pritunl import utils
from pritunl import monitoring

import threading
import flask
import logging
import logging.handlers
import time
import subprocess
import os

app = flask.Flask(__name__)
app_server = None
redirect_app = flask.Flask(__name__ + '_redirect')
acme_token = None
acme_authorization = None
_cur_cert = None
_cur_key = None
_cur_port = None
_update_lock = threading.Lock()
_watch_event = threading.Event()

class CherryPyWSGIServerLogged(wsgiserver.CherryPyWSGIServer):
    def error_log(self, msg='', level=None, traceback=False):
        if not settings.app.log_web_errors:
            return

        if traceback:
            logger.exception(msg, 'app')
        else:
            logger.error(msg, 'app')

def set_acme(token, authorization):
    global acme_token
    global acme_authorization
    acme_token = token
    acme_authorization = authorization

def update_server(delay=0):
    global _cur_cert
    global _cur_key
    global _cur_port

    if not settings.local.server_ready.is_set():
        return

    _update_lock.acquire()
    try:
        if _cur_cert != settings.app.server_cert or \
                _cur_key != settings.app.server_key or \
                _cur_port != settings.app.server_port:
            _cur_cert = settings.app.server_cert
            _cur_key = settings.app.server_key
            _cur_port = settings.app.server_port
            restart_server(delay=delay)
    finally:
        _update_lock.release()

def restart_server(delay=0):
    _watch_event.clear()
    def thread_func():
        time.sleep(delay)
        set_app_server_interrupt()
        if app_server:
            app_server.interrupt = ServerRestart('Restart')
        time.sleep(1)
        clear_app_server_interrupt()
    thread = threading.Thread(target=thread_func)
    thread.daemon = True
    thread.start()

@app.before_request
def before_request():
    flask.g.query_count = 0
    flask.g.write_count = 0
    flask.g.query_time = 0
    flask.g.start = time.time()

@app.after_request
def after_request(response):
    resp_time = int((time.time() - flask.g.start) * 1000)
    db_time = int(flask.g.query_time * 1000)
    db_reads = flask.g.query_count
    db_writes = flask.g.write_count

    response.headers.add('Execution-Time', resp_time)
    response.headers.add('Query-Time', db_time)
    response.headers.add('Query-Count', db_reads)
    response.headers.add('Write-Count', db_writes)

    if not flask.request.path.startswith('/event'):
        monitoring.insert_point('requests', {
            'host': settings.local.host.name,
        }, {
            'path': flask.request.path,
            'remote_ip': utils.get_remote_addr(),
            'response_time': resp_time,
            'db_time': db_time,
            'db_reads': db_reads,
            'db_writes': db_writes,
        })

    return response

@app.route('/.well-known/acme-challenge/<token>', methods=['GET'])
def acme_token_get(token):
    if token == acme_token:
        return flask.Response(acme_authorization, mimetype='text/plain')
    return flask.abort(404)

def _run_server(restart):
    global app_server

    logger.info('Starting server', 'app')

    app_server = CherryPyWSGIServerLogged(
        ('localhost', settings.app.server_internal_port),
        app,
        request_queue_size=settings.app.request_queue_size,
        numthreads=settings.app.request_thread_count,
        shutdown_timeout=3,
    )
    app_server.server_name = ''

    server_cert_path = None
    server_key_path = None
    redirect_server = 'true' if settings.app.redirect_server else 'false'
    internal_addr = 'localhost:' + str(settings.app.server_internal_port)

    if settings.app.server_ssl:
        setup_server_cert()

        server_cert_path, server_key_path = utils.write_server_cert(
            settings.app.server_cert,
            settings.app.server_key,
            settings.app.acme_domain,
        )

    process_state = True
    process = subprocess.Popen(
        ['pritunl-web'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=dict(os.environ, **{
            'REDIRECT_SERVER': redirect_server,
            'BIND_HOST': settings.conf.bind_addr,
            'BIND_PORT': str(settings.app.server_port),
            'INTERNAL_ADDRESS': internal_addr,
            'CERT_PATH': server_cert_path or '',
            'KEY_PATH': server_key_path or '',
        }),
    )

    def poll_thread():
        if process.wait() and process_state:
            time.sleep(0.5)
            if not check_global_interrupt():
                stdout, stderr = process._communicate(None)
                logger.error("Web server process exited unexpectedly", "app",
                    stdout=stdout,
                    stderr=stderr,
                )
                time.sleep(1)
                restart_server(1)
    thread = threading.Thread(target=poll_thread)
    thread.daemon = True
    thread.start()

    if not restart:
        settings.local.server_ready.set()
        settings.local.server_start.wait()

    _watch_event.set()

    try:
        app_server.start()
    except (KeyboardInterrupt, SystemExit):
        return
    except ServerRestart:
        raise
    except:
        logger.exception('Server error occurred', 'app')
        raise
    finally:
        process_state = False
        try:
            process.kill()
        except:
            pass

def _run_wsgi():
    restart = False
    while True:
        try:
            _run_server(restart)
        except ServerRestart:
            restart = True
            logger.info('Server restarting...', 'app')
            continue

def _run_wsgi_debug():
    logger.info('Starting debug server', 'app')

    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.WARNING)
    werkzeug_logger.addFilter(logger.log_filter)
    werkzeug_logger.addHandler(logger.log_handler)

    settings.local.server_ready.set()
    settings.local.server_start.wait()

    try:
        app.run(
            host=settings.conf.bind_addr,
            port=settings.app.server_port,
            threaded=True,
        )
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        logger.exception('Server error occurred', 'app')
        raise

def setup_server_cert():
    if not settings.app.server_dh_params:
        utils.create_server_dh_params()
        settings.commit()

    if not settings.app.server_cert or not settings.app.server_key:
        utils.create_server_cert()
        settings.commit()

def run_server():
    global _cur_cert
    global _cur_key
    global _cur_port
    _cur_cert = settings.app.server_cert
    _cur_key = settings.app.server_key
    _cur_port = settings.app.server_port

    if settings.conf.debug:
        logger.LogEntry(message='Web debug server started.')
    else:
        logger.LogEntry(message='Web server started.')

    if settings.conf.debug:
        _run_wsgi_debug()
    else:
        _run_wsgi()
