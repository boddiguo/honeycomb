# coding: utf-8

import json
import re
import datetime

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from s_mysqllib import dbconfigs, mysqlpoolconnect, mysqlconnect


SQL_COUNT = r'''
    INSERT INTO reporting.pipeline_metrics 
        ( event_time, sender, metrics_name, dimension, value, vcount, create_time, update_time)
    VALUES 
        ( %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP() )
    ON DUPLICATE KEY UPDATE
        update_time = VALUES(update_time),
        vcount = vcount + VALUES(vcount),
        value = value + VALUES(value)
'''

SQL_SUM = r'''
    INSERT INTO reporting.pipeline_metrics 
        ( event_time, sender, metrics_name, dimension, value, vcount, create_time, update_time)
    VALUES 
        ( %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP() )
    ON DUPLICATE KEY UPDATE
        update_time = VALUES(update_time),
        vcount = vcount + VALUES(vcount),
        value = value + VALUES(value)
'''

SQL_MEAN = r'''
    INSERT INTO reporting.pipeline_metrics 
        ( event_time, sender, metrics_name, dimension, value, vcount, create_time, update_time)
    VALUES 
        ( %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP() )
    ON DUPLICATE KEY UPDATE
        update_time = VALUES(update_time),
        vcount = vcount + VALUES(vcount),
        value = (VALUES(value) * VALUES(vcount) + value * vcount) / (VALUES(vcount) + vcount)
'''

SQL_HISTOGRAM = r'''
    INSERT INTO reporting.pipeline_histogram
        ( event_time, sender, metrics_name, dimension, vcount, vsum, vmean, vmin, vmax, p50, p75, p95, p99, p999, create_time, update_time )
    VALUES
        ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP() )
    ON DUPLICATE KEY UPDATE
        update_time = VALUES(update_time),
        vcount = vcount + VALUES(vcount),
        vsum = vsum + VALUES(vsum),
        vmean = (VALUES(vsum) + vsum) / (VALUES(vcount) + vcount)
        vmin = case when VALUES(vmin) < vmin then VALUES(vmin) else vmin end
        vmax = case when VALUES(vmax) > vmax then VALUES(vmax) else vmax end
        p50 =  vcount / (vcount + VALUES(vcount)) * p50 +  VALUES(vcount) / (vcount + VALUES(vcount)) * VALUES(p50)
        p75 =  vcount / (vcount + VALUES(vcount)) * p75 +  VALUES(vcount) / (vcount + VALUES(vcount)) * VALUES(p75)
        p95 =  vcount / (vcount + VALUES(vcount)) * p95 +  VALUES(vcount) / (vcount + VALUES(vcount)) * VALUES(p95)
        p99 =  vcount / (vcount + VALUES(vcount)) * p99 +  VALUES(vcount) / (vcount + VALUES(vcount)) * VALUES(p99)
        p999 = vcount / (vcount + VALUES(vcount)) * p999+  VALUES(vcount) / (vcount + VALUES(vcount)) * VALUES(p999)

'''



class Metrics(object):
    def __init__(self, metrics_name, metrics_type, metrics_config):
        self.metrics_name = metrics_name
        self.metrics_type = metrics_type
        self.metrics_config = metrics_config

        self.dimension_field = self.metrics_config.get('dimensionFields')
        self.dimension_format = self.metrics_config.get('format')
        self.aggregation_field = self.metrics_config.get('aggregationField')
        self.aggregation_func = self.metrics_config.get('aggregationFunc')
        self.filters = self.metrics_config.get('filters')

        self.dimension_func = self._getdimensionfunc(self.dimension_field, self.dimension_format)
        self.filter_func = self._getfilterfunc(self.filters)


    def _getdimensionfunc(self, dimension_field, dimension_format):
        ''' return lambda data: str || None
        '''
        if dimension_field and dimension_format:
            patt = re.compile(dimension_format)
            def _func(data):
                v = data.get(dimension_field)
                if isinstance(v, (str, unicode)) and patt.match(v):
                    return dimension_format
                else:
                    return None
            return _func

        elif dimension_field and not dimension_format:
            return lambda data: data.get(dimension_field)

        else:
            return lambda data: '_ALL_'


    def _getfilterfunc(self, filters):
        ''' return  lambda data: bool
        '''
        def _get_eql(key, value):
            return lambda data: data.get(key) == value

        def _get_in(key, value):
            vset = set(value)
            return lambda data: data.get(key) in vset

        def _get_notin(key, value):
            vset = set(value)
            return lambda data: data.get(key) not in value

        def _get_regex(key, value):
            vreg = re.compile(value)
            return lambda data: isinstance(data.get(key) , (str, unicode)) and vreg.match(data.get(key))

        name_func_map = {
            '=': _get_eql,
            'in': _get_in,
            'notin': _get_notin,
            'regex': _get_regex,
        }

        if not filters:
            return lambda data: True

        elif len(filters) == 1:
            flt = filters[0]
            return name_func_map[flt['operation']] (flt['key'], flt['value'])

        else:
            filter_func_list = [ name_func_map[flt['operation']] (flt['key'], flt['value'])  for flt in filters]
            def _func(data):
                for f in filter_func_list:
                    if not f(data):
                        return False
                return True
            return _func


class MetricsFlow(object):
    def __init__(self):
        self.metricslist = []

    def add(self, sender, event_name, metrics_name, metrics_type, metrics_config):
        metrics = Metrics(metrics_name, metrics_type, metrics_config)
        self.metricslist.append((sender, event_name, metrics))

    def _get_metrics_map(self, metrics_type):
        mmp = {}
        for sender, event_name, metrics in self.metricslist:
            if metrics.metrics_type == metrics_type:
                key = (sender, event_name)
                if key in mmp:
                    mmp[key].append(metrics)
                else:
                    mmp[key] = [metrics]
        return mmp

    def _get_group_ts(self, ts_unix):
        return datetime.datetime.fromtimestamp( int(ts_unix/1000/60) * 60 )

    def pipe_counter(self, p):
        ''' p : stream of data dict
        '''
        mmp = self._get_metrics_map('Counter')

        def _flat(d):
            try:
                timestamp = self._get_group_ts(d['timestamp'])
                sender = d['sender']
                event_name = d['name']
                data = d['data']
                r = []
                for metrics in mmp.get((sender, event_name), []):
                    if metrics.filter_func(data):
                        dimension = metrics.dimension_func(data)
                        if dimension != None:
                            rec = (timestamp, sender, metrics.metrics_name, dimension)
                            r.append((None, rec))
                return r

            except Exception as e:
                e_str = e.__class__.__name__ + str(e.args)
                return [(e_str, d)]


        def _dump_partition(records):
            conn = mysqlpoolconnect('reporting', dbconfigs, maxcached=1)
            cur=conn.cursor()
            for rec in records:
                (timestamp, sender, metrics_name, dimension), vcount = rec
                cur.execute( SQL_COUNT, (timestamp, sender, metrics_name, dimension, vcount, vcount) )

            conn.commit()
            cur.close()
            conn.close()


        p_flat_msg = p.flatMap(_flat)
        p_flat = p_flat_msg.filter(lambda x: x[0] == None).map(lambda x: (x[1], 1))
        p_result = p_flat.reduceByKey(lambda a,b: a+b)
        p_result.foreachRDD(lambda rdd: rdd.foreachPartition(_dump_partition))



    def pipe_gauge(self, p):
        mmp = self._get_metrics_map('Gauge')

        def _flat(d):
            try:
                timestamp = self._get_group_ts(d['timestamp'])
                sender = d['sender']
                event_name = d['name']
                data = d['data']
                r = []
                for metrics in mmp.get((sender, event_name), []):
                    if metrics.filter_func(data):
                        dimension = metrics.dimension_func(data)
                        if dimension != None:
                            aggvalue = data.get(metrics.aggregation_field)
                            if isinstance(aggvalue, (int, long, float)):
                                rec = (timestamp, sender, metrics.metrics_name, dimension, metrics.aggregation_func, aggvalue)
                                r.append((None, rec))
                return r

            except Exception as e:
                e_str = e.__class__.__name__ + str(e.args)
                return [(e_str, d)]


        def _map(x):
            ''' (key, (sum, count) )
            '''
            return (x[:-1], (x[-1], 1) )

        def _reduce(a, b):
            ''' sum, count
            '''
            return (a[0]+b[0], a[1]+b[1] )


        def _dump_partition(records):
            conn = mysqlpoolconnect('reporting', dbconfigs, maxcached=1)
            cur=conn.cursor()
            for rec in records:
                (timestamp, sender, metrics_name, dimension, aggregation_func), (vsum, vcount) = rec

                if aggregation_func == 'count':
                    cur.execute( SQL_COUNT, (timestamp, sender, metrics_name, dimension, vcount, vcount) )
                elif aggregation_func == 'sum':
                    cur.execute( SQL_SUM,   (timestamp, sender, metrics_name, dimension, vsum, vcount) )
                elif aggregation_func == 'mean':
                    cur.execute( SQL_MEAN,  (timestamp, sender, metrics_name, dimension, 1.0 * vsum / vcount, vcount) )
                else:
                    raise Exception('unknown aggregation function "%s"' % aggregation_func)

            conn.commit()
            cur.close()
            conn.close()


        p_flat_msg = p.flatMap(_flat)
        p_flat = p_flat_msg.filter(lambda x: x[0] == None).map(lambda x: x[1]).map(_map)
        p_result = p_flat.reduceByKey(_reduce)
        p_result.foreachRDD(lambda rdd: rdd.foreachPartition(_dump_partition))


    def pipe_histogram(self, p):
        mmp = self._get_metrics_map('Histogram')

        def _flat(d):
            try:
                timestamp = self._get_group_ts(d['timestamp'])
                sender = d['sender']
                event_name = d['name']
                data = d['data']
                r = []
                for metrics in mmp.get((sender, event_name), []):
                    if metrics.filter_func(data):
                        dimension = metrics.dimension_func(data)
                        if dimension != None:
                            aggvalue = data.get(metrics.aggregation_field)
                            if isinstance(aggvalue, (int, long, float)):
                                rec = (timestamp, sender, metrics.metrics_name, dimension, aggvalue)
                                r.append((None, rec))
                return r

            except Exception as e:
                e_str = e.__class__.__name__ + str(e.args)
                return [(e_str, d)]


        def _aggregate(x):
            key, iterator = x
            v = sorted(iterator)
            vcount = len(v)
            vsum = sum(v)
            vmean = 1.0 * vsum / vcount
            vmax = max(v)
            vmin = min(v)
            p50 = v[int(round( (vcount-1)*0.5 ))]
            p75 = v[int(round( (vcount-1)*0.75 ))]
            p95 = v[int(round( (vcount-1)*0.95 ))]
            p99 = v[int(round( (vcount-1)*0.99 ))]
            p999 = v[int(round( (vcount-1)*0.999 ))]

            return key, (vcount, vsum, vmean, vmin, vmax, p50, p75, p95, p99, p999)


        def _dump_partition(records):
            conn = mysqlpoolconnect('reporting', dbconfigs, maxcached=1)
            cur=conn.cursor()
            for rec in records:
                (timestamp, sender, metrics_name, dimension), (vcount, vsum, vmean, vmin, vmax, p50, p75, p95, p99, p999) = rec
                cur.execute( SQL_HISTOGRAM, (timestamp, sender, metrics_name, dimension, vcount, vsum, vmean, vmin, vmax, p50, p75, p95, p99, p999) )

            conn.commit()
            cur.close()
            conn.close()


        p_flat_msg = p.flatMap(_flat)
        p_flat = p_flat_msg.filter(lambda x: x[0] == None).map(lambda x: x[1]).map( lambda x: (x[:-1], x[-1]) )
        p_result = p_flat.groupByKey().map(_aggregate)
        p_result.foreachRDD(lambda rdd: rdd.foreachPartition(_dump_partition))


    def pipe_assembly(self, p):
        def _parse(raw):
            try:
                d = json.loads(raw[1])
                data = json.loads(d['data'])
                return (None, data)
            except Exception as e:
                e_str = e.__class__.__name__ + str(e.args)
                return (e_str, raw)

        p_parsed_msg = p.map(_parse)
        p_parsed = p_parsed_msg.filter(lambda x: x[0] == None).map(lambda x: x[1])
        p_parsed.cache()
        self.pipe_counter(p_parsed)
        self.pipe_gauge(p_parsed)
        self.pipe_histogram(p_parsed)
        p_parsed.count().pprint() # print count


def set_metrics_flow(m):
    conn = mysqlconnect('reporting', dbconfigs)
    cur = conn.cursor()
    cur.execute(''' select x.sender, x.event_name, y.metrics_name, y.metrics_type, y.metrics_config 
                    from reporting.pipeline_event_job x 
                    join reporting.pipeline_event_job_metrics y 
                    on x.id = y.job_id 
                    where x.is_valid = 1 and y.is_valid = 1 ''')
    for sender, event_name, metrics_name, metrics_type, metrics_config in cur.fetchall():
        mconfig = json.loads(metrics_config) if metrics_config != None else {}
        m.add(sender, event_name, metrics_name, metrics_type, mconfig)

    cur.close()
    conn.close()


if __name__ == '__main__':

    m = MetricsFlow()
    set_metrics_flow(m)

    sc = SparkContext(appName="s_data_pipeline.py")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 60)

    kvs = KafkaUtils.createStream(ssc, 'localhost:2181', 's_data_pipeline',  {"common.event.topic": 2})
    m.pipe_assembly(kvs)

    ssc.start()
    ssc.awaitTermination()