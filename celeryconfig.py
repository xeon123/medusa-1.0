import os
import sys

# add hadoop python to the env, just for the running
sys.path.append(os.path.dirname(os.path.basename(__file__)))

# broker configuration
BROKER_URL = "amqp://celeryuser:celery@medusa-rabbitmq/celeryvhost"

CELERY_RESULT_BACKEND = "amqp"
CELERY_RESULT_PERSISTENT = True
TEST_RUNNER = 'celery.contrib.test_runner.run_tests'

# for debug
# CELERY_ALWAYS_EAGER = True

# module loaded
CELERY_IMPORTS = ("medusa.mergedirs", "medusa.medusasystem", "medusa.utility", "medusa.pingdaemon", "medusa.hdfs")