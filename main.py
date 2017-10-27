from datetime import date, datetime, time, timedelta
from json import dumps

import webapp2
from google.appengine.ext import ndb


class ModelUtils(object):
    def to_dict(self):
        result = super(ModelUtils,self).to_dict()
        result['key'] = self.key.id()
        return result

class Sample(ModelUtils, ndb.Model):

    userid = ndb.StringProperty()
    created = ndb.DateTimeProperty(auto_now_add=True)

    namespace = 'watanabe'

    @classmethod
    def query_expire(cls):
        return cls.query(cls.created < datetime.now() - timedelta(minutes=3), namespace=Sample.namespace)


class MainPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.write('app now running!')

def dump_support_default(o):
    if isinstance(o, Sample):
        return o.to_dict()

    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(repr(o) + " is not JSON serializable")

class ShowDataStoreRecords(webapp2.RequestHandler):
    sample = Sample()

    def get(self):
        self.response.headers['Content-Type'] = 'application/json'
        self.response.write(dumps(self.sample.query(namespace=Sample.namespace).fetch(), default=dump_support_default))

class DeleteExpireRecords(webapp2.RequestHandler):

    sample = Sample()

    def get(self):
        count = self.delete_expired_keys()
        self.response.headers['Content-Type'] = 'text/plain'
        if count > 0:
            self.response.write('delete %d expire records.' % (count))
        else:
            self.response.write('no expire records.')

    def delete_expired_keys(self):
        query = self.sample.query_expire()

        delete_keys = []

        for key in query.iter(keys_only=True):
            delete_keys.append(key)

        key_count = len(delete_keys)

        if key_count > 0:
            ndb.delete_multi(delete_keys)

        return key_count

app = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/expire', DeleteExpireRecords),
    ('/datastore', ShowDataStoreRecords)
], debug=True)
