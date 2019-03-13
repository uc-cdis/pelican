class Config(object):
    DEBUG = True
    TESTING = False
    DATABASE_NAME = "papers"


class DevelopmentConfig(Config):
    SECRET_KEY = "S0m3S3cr3tK3y"
    DICTIONARY_URL = "http://s3.amazonaws.com/dictionary-artifacts/datadictionary/develop/schema.json"


config = {
    'development': DevelopmentConfig,
    'testing': DevelopmentConfig,
    'production': DevelopmentConfig
}
