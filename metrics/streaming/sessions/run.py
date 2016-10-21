from subprocess import Popen

def build_hadoop_command(input, output, n_reducers=3, job_name=None):
    return [
        'hadoop', 'jar', '/opt/hadoop/hadoop-streaming.jar',
        '-D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
        '-D mapred.text.key.comparator.options=-k1,2',
        '-D stream.num.map.output.key.fields=2',
        '-D mapred.text.key.partitioner.options=-k1,1',
        '-D mapreduce.job.reduces={}'.format(n_reducers),
        '-D mapred.job.name={}'.format(job_name) if job_name else '',
        '-files', 'map.py,reduce.py',
        '-input', '{}'.format(input),
        '-output', '{}'.format(output),
        '-mapper', './map.py',
        '-partitioner', 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner',
        '-reducer', 'cat'
    ]

def run_map_reduce(input, output, job_name, reducers, cwd):
    return Popen(build_hadoop_command(input, output, n_reducers=reducers, job_name=job_name),
                 cwd=cwd).wait()


