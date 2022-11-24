from pickle import FALSE
import psycopg2

class Config(object):
    SECRET_KEY='5791638bb0b13eg06417'

class ProductionConfig(Config):
    DEBUG=False

class DevelopmentConfig(Config):
    DEBUG=True

db = psycopg2.connect(database="dfqt9vfoh18uko", 
                                     user='kdlnydflpyjhnx',
                                     password='877fc89efc05c0c0f3bc52fbe87ae0b4db0c044cd83c0e14107b42fddb032901',
                                     host='ec2-3-216-167-65.compute-1.amazonaws.com',
                                     port='5432')