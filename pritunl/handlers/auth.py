from pritunl.constants import *
from pritunl import settings
from pritunl import auth
from pritunl import utils
from pritunl import app
from pritunl import event
from pritunl import organization
from pritunl import sso
from pritunl import plugins
from pritunl import logger

import flask
import time
import random

def _auth_radius(username, password):
    valid, org_id = sso.verify_radius(username, password)
    if not valid:
        return utils.jsonify({
            'error': AUTH_INVALID,
            'error_msg': AUTH_INVALID_MSG,
        }, 401)

    if not org_id:
        org_id = settings.app.sso_org

    valid, org_id_new = sso.plugin_sso_authenticate(
        sso_type='radius',
        user_name=username,
        user_email=None,
        remote_ip=utils.get_remote_addr(),
    )
    if valid:
        org_id = org_id_new or org_id
    else:
        logger.error('Radius plugin authentication not valid', 'sso',
            username=username,
        )
        return utils.jsonify({
            'error': AUTH_INVALID,
            'error_msg': AUTH_INVALID_MSG,
        }, 401)

    org = organization.get_by_id(org_id)
    if not org:
        return flask.abort(405)

    usr = org.find_user(name=username)
    if not usr:
        usr = org.new_user(name=username, type=CERT_CLIENT,
            auth_type=RADIUS_AUTH)
        usr.audit_event(
            'user_created',
            'User created with single sign-on',
            remote_addr=utils.get_remote_addr(),
        )

        event.Event(type=ORGS_UPDATED)
        event.Event(type=USERS_UPDATED, resource_id=org.id)
        event.Event(type=SERVERS_UPDATED)
    else:
        if usr.disabled:
            return utils.jsonify({
                'error': AUTH_DISABLED,
                'error_msg': AUTH_DISABLED_MSG,
            }, 403)

        if usr.auth_type != RADIUS_AUTH:
            usr.auth_type = RADIUS_AUTH
            usr.set_pin(None)
            usr.commit(('auth_type', 'pin'))

    key_link = org.create_user_key_link(usr.id, one_time=True)

    usr.audit_event('user_profile',
        'User profile viewed from single sign-on',
        remote_addr=utils.get_remote_addr(),
    )

    return utils.jsonify({
        'redirect': utils.get_url_root()[:-1] + key_link['view_url'],
    }, 202)

def _auth_plugin(username, password):
    valid, org_id = sso.plugin_login_authenticate(
        user_name=username,
        password=password,
        remote_ip=utils.get_remote_addr(),
    )

    if not valid:
        return utils.jsonify({
            'error': AUTH_INVALID,
            'error_msg': AUTH_INVALID_MSG,
        }, 401)

    if not org_id:
        logger.error(
            'Login plugin did not return valid organization name',
            'auth',
            org_name=org_id,
            user_name=username,
        )
        return utils.jsonify({
            'error': AUTH_INVALID,
            'error_msg': AUTH_INVALID_MSG,
        }, 401)

    org = organization.get_by_id(org_id)
    if not org:
        return flask.abort(405)

    usr = org.find_user(name=username)
    if not usr:
        usr = org.new_user(name=username, type=CERT_CLIENT,
            auth_type=PLUGIN_AUTH)
        usr.audit_event(
            'user_created',
            'User created with plugin authentication',
            remote_addr=utils.get_remote_addr(),
        )

        event.Event(type=ORGS_UPDATED)
        event.Event(type=USERS_UPDATED, resource_id=org.id)
        event.Event(type=SERVERS_UPDATED)
    else:
        if usr.disabled:
            return utils.jsonify({
                'error': AUTH_DISABLED,
                'error_msg': AUTH_DISABLED_MSG,
            }, 403)

        if usr.auth_type != PLUGIN_AUTH:
            usr.auth_type = PLUGIN_AUTH
            usr.set_pin(None)
            usr.commit(('auth_type', 'pin'))

    key_link = org.create_user_key_link(usr.id, one_time=True)

    usr.audit_event('user_profile',
        'User profile viewed from plugin authentication',
        remote_addr=utils.get_remote_addr(),
    )

    return utils.jsonify({
        'redirect': utils.get_url_root()[:-1] + key_link['view_url'],
    }, 202)

@app.app.route('/auth/session', methods=['POST'])
def auth_session_post():
    username = flask.request.json['username']
    password = flask.request.json['password']
    otp_code = flask.request.json.get('otp_code')
    remote_addr = utils.get_remote_addr()

    admin = auth.get_by_username(username, remote_addr)
    if not admin:
        if settings.app.sso == RADIUS_AUTH:
            return _auth_radius(username, password)

        time.sleep(random.randint(0, 100) / 1000.)
        return _auth_plugin(username, password)

    if not otp_code and admin.otp_auth:
        return utils.jsonify({
            'error': AUTH_OTP_REQUIRED,
            'error_msg': AUTH_OTP_REQUIRED_MSG,
        }, 402)

    if not admin.auth_check(password, otp_code, remote_addr):
        time.sleep(random.randint(0, 100) / 1000.)
        return utils.jsonify({
            'error': AUTH_INVALID,
            'error_msg': AUTH_INVALID_MSG,
        }, 401)

    flask.session['session_id'] = admin.new_session()
    flask.session['admin_id'] = str(admin.id)
    flask.session['timestamp'] = int(utils.time_now())
    if not settings.app.server_ssl:
        flask.session['source'] = remote_addr

    return utils.jsonify({
        'authenticated': True,
        'default': admin.default or False,
    })

@app.app.route('/auth/session', methods=['DELETE'])
def auth_delete():
    admin_id = flask.session.get('admin_id')
    session_id = flask.session.get('session_id')
    if admin_id and session_id:
        admin_id = utils.ObjectId(admin_id)
        auth.clear_session(admin_id, session_id)
    flask.session.clear()

    return utils.jsonify({
        'authenticated': False,
    })
