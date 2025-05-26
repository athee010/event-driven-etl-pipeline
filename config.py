# config.py

# Load secrets 
secrets = get_secret('your-secret-name')

# DB connection using secrets
conn = psycopg2.connect(
    host=secrets['host'],
    dbname=secrets['dbname'],
    user=secrets['user'],
    password=secrets['password']
)

# S3 config using secrets
BUCKET = secrets['bucket']
PREFIX = secrets['prefix']
