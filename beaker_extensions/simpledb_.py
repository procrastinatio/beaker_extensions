import logging
from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager

try:
    import boto.sdb

except ImportError:
    raise InvalidCacheBackendError("SimpleDB cache backend requires the 'boto' library")

log = logging.getLogger(__name__)

class SimpleDBManager(NoSqlManager):
    """
    Amazon SimpleDB backend for beaker.

    Configuration example:
        beaker.session.type = sdb
        beaker.session.domain = 'test-sdb'
        beaker.session.region = eu-west-1

    The default region is 'eu-west-1'.
    The credentials have to be configured in a boto.cfg file.
    """
    def __init__(self, namespace, url='127.0.0.1:10000', data_dir=None, lock_dir=None, region='eu-west-1', domain=None, **params):
        if not domain:
            raise MissingCacheParameter("domain is required")
        self.region = region
        self.domain_name = domain
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir, lock_dir=lock_dir, **params)

    def open_connection(self, host, port, **params):
        log.debug('open_connection: params=%s', params)
        self.db = boto.sdb.connect_to_region(
           self.region
        )
        
        self.domain = self.db.create_domain(self.domain_name)
        


    def __contains__(self, key):
        return self.bucket.get(self._format_key(key)).exists

    def set_value(self, key, value, expires):
        log.debug('set_value: key=%s, value=%s, expire_time=%s', key, value, expires)
        #val = self.bucket.get(self._format_key(key))
        item = self.domain.get_item(self._format_key(key))
        if not item:
            self.domain.put_attributes(self._format_key(key), value)
        else:
            self.domain.put_attributes(self._format_key(key), value, replace=True)

    def __getitem__(self, key):
        log.debug('__getitem__: key=%s',  self.domain.get_item(self._format_key(key)))
        return self.domain.get_item(self._format_key(key))
        


    def __delitem__(self, key):
        self.domain.delete_item(self._format_key(key))
        
    def _format_key(self, key):
        return 'beaker:%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def do_remove(self):
        raise Exception("Unimplemented")

    def keys(self):
        raise Exception("Unimplemented")


class SimpleDBContainer(Container):
    namespace_class = SimpleDBManager

