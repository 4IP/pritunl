from pritunl.helpers import *
from pritunl import mongo
from pritunl import task
from pritunl import logger

class TaskCleanIpPool(task.Task):
    type = 'clean_ip_pool'

    @cached_static_property
    def pool_collection(cls):
        return mongo.get_collection('servers_ip_pool')

    @cached_static_property
    def server_collection(cls):
        return mongo.get_collection('servers')

    def task(self):
        server_orgs = {}
        for doc in self.server_collection.find({}, {
                    '_id': True,
                    'organizations': True,
                }):
            server_orgs[doc['_id']] = doc.get('organizations') or []

        server_ids = list(server_orgs.keys())

        self.pool_collection.delete_many({
            'server_id': {'$nin': server_ids},
        })

        for server_id, org_ids in list(server_orgs.items()):
            candidate_org_ids = self.pool_collection.find({
                'server_id': server_id,
                'user_id': {'$exists': True},
                'org_id': {
                    '$exists': True,
                    '$nin': org_ids,
                },
            }, {
                'org_id': True,
            }).distinct('org_id')
            if not candidate_org_ids:
                continue

            doc = self.server_collection.find_one({
                '_id': server_id,
            }, {
                'organizations': True,
            })
            if not doc:
                continue
            cur_org_ids = set(doc.get('organizations') or [])

            detached_org_ids = [org_id for org_id in candidate_org_ids
                if org_id not in cur_org_ids]
            if not detached_org_ids:
                continue

            response = self.pool_collection.update_many({
                'server_id': server_id,
                'user_id': {'$exists': True},
                'org_id': {'$in': detached_org_ids},
            }, {'$unset': {
                'org_id': '',
                'user_id': '',
            }})

            if response.modified_count:
                logger.warning('Unassigned ip addresses from ' +
                    'detached orgs', 'tasks',
                    server_id=server_id,
                    org_ids=detached_org_ids,
                    count=response.modified_count,
                )

        response = self.pool_collection.aggregate([
            {'$match': {
                'user_id': {'$exists': True},
            }},
            {'$group': {
                '_id': {
                    'server_id': '$server_id',
                    'network': '$network',
                    'user_id': '$user_id',
                },
                'docs': {'$addToSet': '$_id'},
                'count': {'$sum': 1},
            }},
            {'$match': {
                'count': {'$gt': 1},
            }},
        ])

        for doc in response:
            server_id = doc['_id']['server_id']
            user_id = doc['_id']['user_id']
            network = doc['_id']['network']
            doc_ids = doc['docs'][1:]

            for doc_id in doc_ids:
                self.pool_collection.update_one({
                    '_id': doc_id,
                    'server_id': server_id,
                    'network': network,
                    'user_id': user_id,
                }, {'$unset': {
                    'org_id': '',
                    'user_id': '',
                }})

        response = self.pool_collection.aggregate([
            {'$match': {
                'user_id': {'$exists': True},
            }},
            {'$lookup': {
                'from': mongo.prefix + 'users',
                'localField': 'user_id',
                'foreignField': '_id',
                'as': 'user_docs',
            }},
            {'$match': {
                'user_docs': {'$size': 0},
            }},
        ])

        for doc in response:
            self.pool_collection.update_one({
                '_id': doc['_id'],
                'user_id': doc['user_id'],
            }, {'$unset': {
                'org_id': '',
                'user_id': '',
            }})

task.add_task(TaskCleanIpPool, hours=5, minutes=23)
