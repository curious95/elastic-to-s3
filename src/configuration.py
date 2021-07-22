import os


class Config:
    """
    Configuration class to store all the env vars
    """

    def __init__(self):
        self.index = os.getenv('INDEX', 'xx')
        self.es_user = os.getenv('ESUSER', 'xx')
        self.es_host = os.getenv('ESHOST', 'xx')
        self.es_pass = os.getenv('ESPASS', 'xx')
        self.es_port = os.getenv('ESPORT', 'xx')
        self.bucket = os.getenv('BUCKET', 'xx')
        self.topic = os.getenv('TOPIC', 'xx')
        self.region = os.getenv('REGION', 'xx')