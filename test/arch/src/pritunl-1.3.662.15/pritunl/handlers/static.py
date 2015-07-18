from pritunl.constants import *
from pritunl.exceptions import *
from pritunl.helpers import *
from pritunl import app
from pritunl import settings
from pritunl import static
from pritunl import auth
from pritunl import utils

import flask

@app.app.route('/s/', methods=['GET'])
@app.app.route('/s/<path:file_path>', methods=['GET'])
@auth.session_auth
def static_get(file_path=None):
    if settings.local.dart_url:
        file_path = file_path or 'index.html'
        response = utils.request.get(settings.local.dart_url + file_path)
        return flask.Response(response.content, headers=response.headers)

    if not file_path:
        return flask.abort(404)

    try:
        static_file = static.StaticFile(settings.conf.www_path,
            file_path, cache=True)
    except InvalidStaticFile:
        return flask.abort(404)

    return static_file.get_response()

@app.app.route('/favicon.ico', methods=['GET'])
def favicon_static_get():
    static_file = static.StaticFile(settings.conf.www_path,
        'favicon.ico', cache=True)

    return static_file.get_response()

@app.app.route('/robots.txt', methods=['GET'])
def robots_static_get():
    static_file = static.StaticFile(settings.conf.www_path,
        'robots.txt', cache=True)

    return static_file.get_response()

@app.app.route('/', methods=['GET'])
def index_static_get():
    if not auth.check_session():
        return flask.redirect('login')

    if settings.local.dart_url:
        return flask.redirect('/s/')

    static_file = static.StaticFile(settings.conf.www_path,
        'index.html', cache=False)

    return static_file.get_response()

@app.app.route('/login', methods=['GET'])
def login_static_get():
    if auth.check_session():
        return flask.redirect('')
    static_file = static.StaticFile(settings.conf.www_path,
        'login.html', cache=False)

    bodyClass = ''

    if settings.local.sub_active:
        if settings.app.theme == 'dark':
            bodyClass += 'dark '

        if settings.app.sso and settings.local.sub_plan == 'enterprise':
            bodyClass += 'sso '

    static_file.data = static_file.data.replace(
        '<body>', '<body class="' + bodyClass + '">')

    return static_file.get_response()
