"""
Collects data from Cassandra through the nodetool command-line interface

#### Dependencies

 * re
 * subprocess

"""

import diamond.collector
import subprocess
import re

class CassandraNodetoolCollector(diamond.collector.Collector):
    def get_default_config_help(self):
        config_help = super(CassandraNodetoolCollector, self).get_default_config_help()
        config_help.update({
            'nodetool' : 'path to nodetool',
            'host' : 'host to run nodetool against'
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(CassandraNodetoolCollector, self).get_default_config()
        config.update(  {
            'nodetool': 'nodetool',
            'host': 'localhost'
        } )
        return config

    def sanitize(self, key):
        return key.replace(" ", "_").replace("(", "").replace(")", "")
        
    def publish_if_number(self, key, value):
        try:
            number = float(value)
            self.publish(self.sanitize(key), number)
        except ValueError:
            return
            
    def parse_space_seperated(self, lines, prefex, row_headers, skip_lines):
        for row_index in range(len(lines)):
            if row_index in skip_lines:
                continue
            
            line = lines[row_index]
            words = re.split('\s{2,}', line)
            if row_index in row_headers:
                column_headers = words[1:]
                continue
            
            if len(words) == (1 + len(column_headers)):
                row_header = words[0]
                for i in range(len(column_headers)):
                    yield ".".join((prefex, row_header, column_headers[i])), words[1 + i]

    def publish_tabular_stats(self, node_tool_command, row_headers = [0], skip_lines = []):
        nodetool = self.config.get("nodetool")
        host = self.config.get("host")
        lines = subprocess.check_output([nodetool, "-h", host, node_tool_command]).splitlines()
        for key, value in self.parse_space_seperated(lines, node_tool_command, row_headers, skip_lines):
            self.publish_if_number(key, value)

    def publish_columnar_stats(self, node_tool_command, regex="\s*([^:]+\S)\s*:\s+(.*)", value_regexes_by_key={}):
        nodetool = self.config.get("nodetool")
        host = self.config.get("host")
        lines = subprocess.check_output([nodetool, "-h", host, node_tool_command]).splitlines()
        for line in lines:
            m = re.match(regex, line)
            if m and len(m.groups()) == 2:
                key = m.groups()[0]
                value = m.groups()[1]
                if key in value_regexes_by_key:
                    for suffix, value_regex in value_regexes_by_key[key].iteritems():
                        m2 = re.search(value_regex, value)
                        if m2:
                            self.publish_if_number(node_tool_command + "." + key + suffix, m2.groups()[0])
                else:
                    self.publish_if_number(node_tool_command + "." + key, value)

    def publish_list_stats(self, node_tool_command, regex="\s*([^:]+\S)\s*:\s+(.*)", numeric_value_regex = "(\d+)", key_prefex_headers=[], key_includes=None, key_excludes=None, prefex_value_excludes=None):
        nodetool = self.config.get("nodetool")
        host = self.config.get("host")
        lines = subprocess.check_output([nodetool, "-h", host, node_tool_command]).splitlines()
        prefexes = []
        for line in lines:
            m = re.match(regex, line)
            if m and len(m.groups()) == 2:
                key = m.groups()[0]
                value = m.groups()[1]
                if key in key_prefex_headers:
                    prefex_index = key_prefex_headers.index(key)
                    prefexes = prefexes[:prefex_index] + [value]
                    continue
                
                if key_includes is not None and key not in key_includes:
                    continue
                if key_excludes is not None and key in key_excludes:
                    continue
                
                if prefex_value_excludes is not None and len(set(prefex_value_excludes).intersection(set(prefexes))) > 0:
                    continue
                
                m2 = re.search(numeric_value_regex, value)
                if m2:
                    value = m2.groups()[0]
                self.publish_if_number(".".join([node_tool_command] + prefexes + [key]), value)

    def collect(self):
        try:
            self.publish_tabular_stats("netstats", skip_lines = [0,1,2], row_headers=[3])
            self.publish_tabular_stats("tpstats", row_headers=[0,17])
            cache_regexes = { ".size": "size (\d+) \(bytes\)", ".capacity": "capacity (\d+) \(bytes\)", ".hits": "(\d+) hits", ".requests": "(\d+) requests", ".recent hit rate": "([\d\.]+) recent hit rate" }
            self.publish_columnar_stats("info", value_regexes_by_key={"Load" : { "_GB": "([\d\.]+) GB" }, "Heap Memory (MB)" : { "": "([\d\.]+) / [\d\.]+" }, "Key Cache" : cache_regexes, "Row Cache" : cache_regexes })
            self.publish_list_stats("cfstats", key_prefex_headers=["Keyspace", "Column Family"], key_includes=["Read Count", "Read Latency", "Write Count", "Write Latency", "SSTable count", "Space used (total)", "Number of Keys (estimate)", "Pending Tasks"], prefex_value_excludes=["OpsCenter", "system"])

        except Exception, e:
            self.log.error('Couldnt connect to cassandra nodetool %s', e)
            return {}
