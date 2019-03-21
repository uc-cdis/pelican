class Config(object):
    DEBUG = True
    TESTING = False


class DevelopmentConfig(Config):
    DEBUG = False
    TESTING = False
    SECRET_KEY = "S0m3S3cr3tK3y"
    DICTIONARY_URL = "http://s3.amazonaws.com/dictionary-artifacts/datadictionary/develop/schema.json"
    DB_URL = 'jdbc:postgresql://localhost/metadata_db'
    DB_USER = 'postgres'
    DB_PASS = 'postgres'


config = {
    'development': DevelopmentConfig,
    'testing': DevelopmentConfig,
    'production': DevelopmentConfig
}
