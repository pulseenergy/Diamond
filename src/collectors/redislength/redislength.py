# coding=utf-8

"""
Collects redis key lengths from one or more Redis Servers
Based on RedisCollector

#### Dependencies

 * redis

#### Notes

Example config file RedisLengthCollector.conf

```
enabled=True
host=redis.example.com
port=16379
keys=set1,string1,string2,sortedset1
```

or for multi-instance mode:

```
enabled=True
instances = nick1@host1:port1, nick2@host2:port2, ...
keys=set1,string1,string2,sortedset1
```

Note: when using the host/port config mode, the port number is used in
the metric key. When using the multi-instance mode, the nick will be used.
If not specified the port will be used.


"""

import diamond.collector
import time

try:
    import redis
    redis  # workaround for pyflakes issue #13
except ImportError:
    redis = None

class RedisLengthCollector(diamond.collector.Collector):

    _DATABASE_COUNT = 16
    _DEFAULT_DB = 0
    _DEFAULT_HOST = 'localhost'
    _DEFAULT_PORT = 6379
    _DEFAULT_SOCK_TIMEOUT = 5

    def __init__(self, *args, **kwargs):
        super(RedisLengthCollector, self).__init__(*args, **kwargs)

        instance_list = self.config['instances']
        # configobj make str of single-element list, let's convert
        if isinstance(instance_list, basestring):
            instance_list = [instance_list]

        # process original single redis instance
        if len(instance_list) == 0:
            host = self.config['host']
            port = self.config['port']
            instance_list.append('%s:%s' % (host, port))

        self.instances = {}
        for instance in instance_list:

            if '@' in instance:
                (nickname, hostport) = instance.split('@', 2)
            else:
                nickname = None
                hostport = instance

            if ':' in hostport:
                if hostport[0] == ':':
                    host = self._DEFAULT_HOST
                    port = int(hostport[1:])
                else:
                    parts = hostport.split(':')
                    host = parts[0]
                    port = int(parts[1])
            else:
                host = hostport
                port = self._DEFAULT_PORT

            if nickname is None:
                nickname = str(port)

            self.instances[nickname] = (host, port)
        self.length_functions = {'list': 'llen', 'set': 'scard', 'zset': 'zcard', 'hash': 'hlen', 'string': 'strlen'}

    def get_default_config_help(self):
        config_help = super(RedisLengthCollector, self).get_default_config_help()
        config_help.update({
            'host': 'Hostname to collect from',
            'port': 'Port number to collect from',
            'timeout': 'Socket timeout',
            'db': '',
            'databases': 'how many database instances to collect',
            'instances': "Redis addresses, comma separated, syntax:"
            + " nick1@host:port, nick2@:port or nick3@host",
            'keys': 'Keys to collect length of, comma separated'
        })
        return config_help

    def get_default_config(self):
        """
        Return default config

:rtype: dict

        """
        config = super(RedisLengthCollector, self).get_default_config()
        config.update({
            'host': self._DEFAULT_HOST,
            'port': self._DEFAULT_PORT,
            'timeout': self._DEFAULT_SOCK_TIMEOUT,
            'db': self._DEFAULT_DB,
            'databases': self._DATABASE_COUNT,
            'path': 'redis',
            'instances': [],
            'keys': ''
        })
        return config

    def _client(self, host, port):
        """Return a redis client for the configuration.

:param str host: redis host
:param int port: redis port
:rtype: redis.Redis

        """
        db = int(self.config['db'])
        timeout = int(self.config['timeout'])
        try:
            cli = redis.Redis(host=host, port=port,
                              db=db, socket_timeout=timeout)
            cli.ping()
            return cli
        except Exception, ex:
            self.log.error("RedisLengthCollector: failed to connect to %s:%i. %s.",
                           host, port, ex)

    def _precision(self, value):
        """Return the precision of the number

:param str value: The value to find the precision of
:rtype: int

        """
        value = str(value)
        decimal = value.rfind('.')
        if decimal == -1:
            return 0
        return len(value) - decimal - 1

    def _publish_key(self, nick, key):
        """Return the full key for the partial key.

:param str nick: Nickname for Redis instance
:param str key: The key name
:rtype: str

        """
        return '%s.%s.%s' % (nick, "length", key)

    def _get_length(self, client, key):
        """Return length of the key 

:param redis.Redis client: redis client
:param str key: redis key
:rtype: int

        """
        key_type = client.type(key)
        if key_type not in self.length_functions:
            return None
        length = getattr(client, self.length_functions[key_type])(key)
        return length
        
    def collect_instance(self, nick, host, port):
        """Collect metrics from a single Redis instance

:param str nick: nickname of redis instance
:param str host: redis host
:param int port: redis port

        """

        # Connect to redis
        client = self._client(host, port)
        if client is None:
            return
        data = dict()

        for key in self.config['keys']:
            if key is not None and len(key) > 0:
                length = self._get_length(client, key)
                if length is not None:
                    data[key] = length
        
        del client

        # Publish the data to graphite
        for key in data:
            self.publish(self._publish_key(nick, key),
                         data[key],
                         self._precision(data[key]))

    def collect(self):
        """Collect the stats from the redis instance and publish them.

        """
        if redis is None:
            self.log.error('Unable to import module redis')
            return {}

        for nick in self.instances.keys():
            (host, port) = self.instances[nick]
            self.collect_instance(nick, host, int(port))
