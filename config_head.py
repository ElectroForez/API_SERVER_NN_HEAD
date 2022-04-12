from os import environ
DB_PATH = 'db_head.db'
API_PASSWORD = environ['PASS_API_NN_HEAD']
SERVERS_PATH = 'servers.txt'
# waiting time
MAX_UPLOAD_TIME = 100000  # secs
MAX_DOWNLOAD_TIME = 100000  # secs
MAX_PROCESSING_TIME = 100000  # secs
MAX_NOT_AVAILABLE = 1000  # secs
MAX_PARALLEL_UPLOAD = 3
MAX_PARALLEL_DOWNLOAD = 2

