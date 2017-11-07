from datetime import date, datetime, time, timedelta
from json import dumps

import webapp2
from google.appengine.ext import ndb


class ModelUtils(object):
    def to_dict(self):
        result = super(ModelUtils,self).to_dict()
        result['key'] = self.key.id()
        return result

class user_classification(ModelUtils, ndb.Model):

    user_id = ndb.StringProperty()
    insert_time = ndb.DateTimeProperty(auto_now_add=True)
    from_streaming = ndb.BooleanProperty()

    namespace = 'csx'

    @classmethod
    def query_expire(cls):
#       return cls.query(ndb.AND(cls.insert_time < datetime.now() - timedelta(minutes=3), cls.from_streaming == True), namespace=user_classification.namespace)
        return cls.query(cls.insert_time < datetime.now() - timedelta(minutes=3), namespace=user_classification.namespace)

class MainPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.write('app now running!')

def dump_support_default(o):
    if isinstance(o, user_classification):
        return o.to_dict()

    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(repr(o) + " is not JSON serializable")

class ShowDataStoreRecords(webapp2.RequestHandler):
    classification = user_classification()

    def get(self):
        self.response.headers['Content-Type'] = 'application/json'
        self.response.write(dumps(self.classification.query(namespace=user_classification.namespace).fetch(), default=dump_support_default))

class DeleteExpireRecords(webapp2.RequestHandler):

    classification = user_classification()

    def get(self):
        count = self.delete_expired_keys()
        self.response.headers['Content-Type'] = 'text/plain'
        if count > 0:
            self.response.write('delete %d expire records.' % (count))
        else:
            self.response.write('no expire records.')

    def delete_expired_keys(self):
        rows = self.classification.query_expire()

        delete_keys = []
        
#       for row in rows.iter(keys_only=True):
#           delete_keys.append(row)
        for row in rows.iter():
            if row.from_streaming == True:
                delete_keys.append(row.key)

        key_count = len(delete_keys)

        if key_count > 0:
            ndb.delete_multi(delete_keys)

        return key_count

app = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/expire', DeleteExpireRecords),
    ('/datastore', ShowDataStoreRecords)
], debug=True)
