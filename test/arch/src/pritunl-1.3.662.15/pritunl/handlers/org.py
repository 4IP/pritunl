from pritunl.constants import *
from pritunl.exceptions import *
from pritunl.helpers import *
from pritunl import logger
from pritunl import utils
from pritunl import event
from pritunl import organization
from pritunl import app
from pritunl import auth

import flask

@app.app.route('/organization', methods=['GET'])
@app.app.route('/organization/<org_id>', methods=['GET'])
@auth.session_auth
def org_get(org_id=None):
    if org_id:
        return utils.jsonify(organization.get_by_id(org_id).dict())

    orgs = []
    page = flask.request.args.get('page', None)
    page = int(page) if page else page

    for org in organization.iter_orgs(page=page):
        orgs.append(org.dict())

    if page is not None:
        return utils.jsonify({
            'page': page,
            'page_total': organization.get_org_page_total(),
            'organizations': orgs,
        })
    else:
        return utils.jsonify(orgs)

@app.app.route('/organization', methods=['POST'])
@auth.session_auth
def org_post():
    name = utils.filter_str(flask.request.json['name'])
    org = organization.new_org(name=name, type=ORG_DEFAULT)
    logger.LogEntry(message='Created new organization "%s".' % org.name)
    event.Event(type=ORGS_UPDATED)
    return utils.jsonify(org.dict())

@app.app.route('/organization/<org_id>', methods=['PUT'])
@auth.session_auth
def org_put(org_id):
    org = organization.get_by_id(org_id)
    name = utils.filter_str(flask.request.json['name'])
    org.name = name
    org.commit(org.changed)
    event.Event(type=ORGS_UPDATED)
    return utils.jsonify(org.dict())

@app.app.route('/organization/<org_id>', methods=['DELETE'])
@auth.session_auth
def org_delete(org_id):
    org = organization.get_by_id(org_id)
    name = org.name
    server_ids = org.remove()

    logger.LogEntry(message='Deleted organization "%s".' % name)

    for server_id in server_ids:
        event.Event(type=SERVER_ORGS_UPDATED, resource_id=server_id)
    event.Event(type=SERVERS_UPDATED)
    event.Event(type=ORGS_UPDATED)

    return utils.jsonify({})
