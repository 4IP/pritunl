from pritunl.user.user import User

from pritunl.constants import *
from pritunl.exceptions import *
from pritunl.helpers import *

import threading

def new_pooled_user(org, type):
    type = {
        CERT_SERVER: CERT_SERVER_POOL,
        CERT_CLIENT: CERT_CLIENT_POOL,
    }[type]

    thread = threading.Thread(target=org.new_user, kwargs={
        'type': type,
        'block': False,
    })
    thread.daemon = True
    thread.start()

def reserve_pooled_user(org, name=None, email=None, type=CERT_CLIENT,
        auth_type=None, disabled=None, resource_id=None):
    doc = {}

    if name is not None:
        doc['name'] = name
    if email is not None:
        doc['email'] = email
    if type is not None:
        doc['type'] = type
    if auth_type is not None:
        doc['auth_type'] = auth_type
    if disabled is not None:
        doc['disabled'] = disabled
    if resource_id is not None:
        doc['resource_id'] = resource_id

    doc = User.collection.find_and_modify({
        'org_id': org.id,
        'type': {
            CERT_SERVER: CERT_SERVER_POOL,
            CERT_CLIENT: CERT_CLIENT_POOL,
        }[type],
    }, {
        '$set': doc,
    }, new=True)

    if doc:
        return User(org=org, doc=doc)

def get_user(org, id, fields=None):
    return User(org=org, id=id, fields=fields)

def find_user(org, name=None, type=None, resource_id=None):
    spec = {
        'org_id': org.id,
    }
    if name is not None:
        spec['name'] = name
    if type is not None:
        spec['type'] = type
    # TODO check explain for resource_id
    if resource_id is not None:
        spec['resource_id'] = resource_id
    return User(org, spec=spec)
