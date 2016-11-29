from subprocess import Popen

def build_spark_command(output, input):
    return [
        'spark-submit', 'likes.py',
        output, input[0], input[1], input[2],
        '--master', 'yarn-cluster',
        '--num-executors', '20',
        '--executor-cores', '1',
        '--executor-memory', '2048m'
    ]

def run_spark(input, output, cwd):
    return Popen(build_spark_command(output, input), cwd=cwd).wait()


