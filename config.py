creds = "admin:Admin@123"
url = "192.168.0.107"
admin_port = 15672
api_port = 5672
host = f'amqp://{creds}@{url}:{api_port}/%2F'
queue = 'tred'
