"""
Collects data from nginx access log

#### Dependencies

 * re

"""

import diamond.collector
import os
import re

class NginxLogCollector(diamond.collector.Collector):
    
    def __init__(self, *args, **kwargs):
        super(NginxLogCollector, self).__init__(*args, **kwargs)
        self.last_log_bytes = {}
        self.last_unique_sessions = []
        for i in range(15):
            self.last_unique_sessions.append(set())
    
    def get_default_config_help(self):
        config_help = super(NginxLogCollector, self).get_default_config_help()
        config_help.update({
            'access_logs' : 'nginx access logs (comma separated)',
            'previous_log_suffix' : 'suffix for finding the previous log (for rollovers)',
            'log_regex' : 'regular expression for parsing a line from the access log - grouped by field',
            'regex_group_session' : 'index of the session id in the regular express',
            'regex_group_response_time' : 'index of the response time in the regular express',
            'regex_group_response_size' : 'index of the response size in the regular express',
            'regex_group_response_status' : 'index of the response status code in the regular express',
            'regex_group_http_method' : 'index of the http method in the regular express'
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(NginxLogCollector, self).get_default_config()
        config.update(  {
            'access_logs': '/var/log/nginx/access.log',
            'previous_log_suffix': '.1',
            'log_regex': '',
            'regex_group_session': '',
            'regex_group_response_time': -1,
            'regex_group_response_size': -1,
            'regex_group_response_status': -1,
            'regex_group_http_method' : -1
        } )
        return config

    def sanitize(self, key):
        return key.replace(" ", "_").replace("(", "").replace(")", "")
        
    def publish_if_non_zero(self, key, number):
        if number != 0:
            self.publish(self.sanitize(key), number)

    def parse_number(self, regexMatch, groupIndex):
        try:
            return float(regexMatch.groups()[groupIndex])
        except ValueError:
            return 0
            
    def yield_line(self, log_file):
        previous_log_suffix = self.config.get("previous_log_suffix")
        try:
            file_size = os.path.getsize(log_file)
            if log_file not in last_log_bytes:
                last_log_bytes[log_file] = file_size
            else:
                if last_log_bytes[log_file] > file_size:
                    # File rolled over. Read previous file if it exists.
                    if previous_log_suffix:
                        try:
                            with open(log_file + previous_log_suffix, "r") as previous_log_file:
                                previous_log_file.seek(last_log_bytes[log_file])
                                for log_line in previous_log_file:
                                    yield log_line
                        except Exception, e:
                            self.log.error("Cannot find previous nginx access log %s: %s", log_file + previous_log_suffix, e)
                    last_log_bytes[log_file] = 0
                with open(log_file, "r") as current_log_file:
                    current_log_file.seek(last_log_bytes[log_file])
                    for log_line in current_log_file:
                        yield log_line
                    last_log_bytes[log_file] = current_log_file.tell()
        except Exception, e:
            self.log.error("Cannot find nginx access log %s: %s", log_file, e)
    
    def increment_dict(self, dictionary, key, incr=1):
        if not key in dictionary:
            dictionary[key] = incr
        else:
            dictionary[key] += incr
    
    def collect(self):
        try:
            access_logs = self.config.get("access_logs").split(",")
            regex = re.compile(self.config.get("log_regex"))
            session_regex_index = int(self.config.get("regex_group_session"))
            response_time_regex_index = int(self.config.get("regex_group_response_time"))
            response_size_regex_index = int(self.config.get("regex_group_response_size"))
            response_status_regex_index = int(self.config.get("regex_group_response_status"))
            http_method_regex_index = int(self.config.get("regex_group_http_method"))
            self.last_unique_sessions.pop(0)
            unique_sessions = set()
            count = 0
            response_time_total = 0
            response_size_total = 0
            response_status_counts = {}
            http_methods_counts = {}
            for access_log in access_logs:
                if access_log not in self.last_log_bytes:
                    try:
                        self.last_log_bytes[access_log] = os.path.getsize(access_log)
                    except os.error, e:
                        self.log.error("Cannot find nginx access log %s: %s", access_log, e)
                    continue
                log_file = open(access_log, "r")
                log_file.seek(self.last_log_bytes[access_log])
                for log_line in log_file:
                    m = regex.match(log_line)
                    if m is None or len(m.groups()) == 0:
                        continue
                    count += 1
                    if session_regex_index >= 0:
                        session = m.groups()[session_regex_index]
                        if len(session) > 0:
                            unique_sessions.add(session)
                    if response_time_regex_index >= 0:
                        response_time_total += self.parse_number(m, response_time_regex_index)
                    if response_size_regex_index >= 0:
                        response_size_total += self.parse_number(m, response_size_regex_index)
                    if response_status_regex_index >= 0:
                        response_status = m.groups()[response_status_regex_index]
                        self.increment_dict(response_status_counts, response_status)
                    if http_method_regex_index >= 0:
                        http_method = m.groups()[http_method_regex_index]
                        self.increment_dict(http_methods_counts, http_method)
                self.last_log_bytes[access_log] = log_file.tell()
            self.last_unique_sessions.append(unique_sessions)
            
            self.publish_if_non_zero("requests", count)
            self.publish_if_non_zero("avg_response_time", response_time_total * 1000 / count if count > 0 else 0)
            self.publish_if_non_zero("avg_response_size", response_size_total / count if count > 0 else 0)
            self.publish_if_non_zero("unique_sessions_1", len(set.union(*self.last_unique_sessions[-1:])))
            self.publish_if_non_zero("unique_sessions_5", len(set.union(*self.last_unique_sessions[-5:])))
            self.publish_if_non_zero("unique_sessions_15", len(set.union(*self.last_unique_sessions[-15:])))
            for response_status, response_count in response_status_counts.iteritems():
                self.publish("response_status." + response_status, response_count)
            for http_method, count in http_methods_counts.iteritems():
                self.publish("http_method." + http_method, count)
            
        except Exception, e:
            self.log.error('Error parsing nginx access log: %s', e)
            return {}
