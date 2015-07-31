from pritunl.helpers import *
from pritunl import mongo
from pritunl import task

class TaskCleanIpPool(task.Task):
    type = 'clean_ip_pool'

    @cached_static_property
    def pool_collection(cls):
        return mongo.get_collection('servers_ip_pool')

    @cached_static_property
    def server_collection(cls):
        return mongo.get_collection('servers')

    def task(self):
        org_ids = self.server_collection.find({}, {
            '_id': True,
        }).distinct('_id')

        self.pool_collection.remove({
            'server_id': {'$nin': org_ids},
        })

task.add_task(TaskCleanIpPool, hours=5, minutes=23)
