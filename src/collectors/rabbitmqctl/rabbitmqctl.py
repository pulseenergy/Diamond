"""
Collects data from RabbitMQ through the command-line interface

#### Dependencies

 * subprocess

"""

import diamond.collector
import subprocess

class RabbitMQCtlCollector(diamond.collector.Collector):
    def get_default_config_help(self):
        config_help = super(RabbitMQCtlCollector, self).get_default_config_help()
        config_help.update({
            'rabbitmqctl' : 'path to rabbitmqctl'
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(RabbitMQCtlCollector, self).get_default_config()
        config.update(  {
            'rabbitmqctl':     'rabbitmqctl'
        } )
        return config

    def sanitize(self, vhost):
        return vhost.replace("/", "_").replace(" ", "_")
        
    def collect(self):
        # if we have a lot of individual queues, only report aggregate
        MAX_INDIVIDUAL_QUEUE_STATS_PER_VHOST = 20
        try:
            rabbitmqctl = self.config.get("rabbitmqctl")
            num_connections = len(subprocess.check_output([rabbitmqctl, "-q", "list_connections"]).splitlines())
            self.publish("connections", num_connections)
            
            vhosts = subprocess.check_output([rabbitmqctl, "-q", "list_vhosts"]).splitlines()
            totalMessages = 0
            totalExchanges = 0
            totalBindings = 0
            for vhost in vhosts:
                messages = 0
                queue_stats = subprocess.check_output([rabbitmqctl, "-q", "list_queues", "-p", vhost]).splitlines()
                for queue_sizes in queue_stats:
                    queue, value = queue_sizes.split("\t")
                    size = int(value)
                    messages += size
                    if len(queue_stats) <= MAX_INDIVIDUAL_QUEUE_STATS_PER_VHOST:
                        self.publish("messages." + self.sanitize(vhost) + "." + queue, size)
                self.publish("messages." + self.sanitize(vhost) + ".total", messages)
                totalMessages += messages
                
                exchanges = len(subprocess.check_output([rabbitmqctl, "-q", "list_exchanges", "-p", vhost]).splitlines())
                self.publish("exchanges." + self.sanitize(vhost) + ".total", exchanges)
                totalExchanges += exchanges
                
                bindings = len(subprocess.check_output([rabbitmqctl, "-q", "list_bindings", "-p", vhost]).splitlines())
                self.publish("bindings." + self.sanitize(vhost) + ".total", bindings)
                totalBindings += bindings
            self.publish("messages.total", totalMessages)
            self.publish("exchanges.total", totalExchanges)
            self.publish("bindings.total", totalBindings)

        except Exception, e:
            self.log.error('Couldnt connect to rabbitmqctl %s', e)
            return {}
