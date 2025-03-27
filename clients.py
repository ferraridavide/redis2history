from contextlib import suppress
from multiprocessing.managers import BaseProxy
import threading
import time
import types

import pymongo
from models import RedisSettings, MongoSettings
from redis import Redis
from rediscluster import RedisCluster

from redis.lock import Lock
from redis.exceptions import LockError

# Indica il moltiplicatore usato per ottenere il tempo di attesa per il rilascio automatico del lock
# rispetto al timeout del lock stesso
LOCK_REACQUIRE_MULTIPLIER = 0.75

class ReacquiringLock(Lock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.auto_reacquire_timeout = self.timeout * LOCK_REACQUIRE_MULTIPLIER
        self.auto_reacquiring_thread = None
        self.stop_auto_reacquire = threading.Event()

    def _auto_reacquire(self):
        while not self.stop_auto_reacquire.is_set():
            time.sleep(self.auto_reacquire_timeout)
            with suppress(LockError):
                super().reacquire()

    def acquire(self, *args, **kwargs):
        acquired = super().acquire(*args, **kwargs)
        if acquired:
            self.auto_reacquiring_thread = threading.Thread(target=self._auto_reacquire)
            self.auto_reacquiring_thread.start()
        return acquired

    def release(self):
        if self.auto_reacquiring_thread:
            self.stop_auto_reacquire.set()
            self.auto_reacquiring_thread.join()

        super().release()


class RedisClient:    
    def __init__(self, s:RedisSettings):
        if s.cluster:
            self.connection = RedisCluster(host=s.host, port=s.port, ssl=s.tls, decode_responses=True,
                                      skip_full_coverage_check=True)
        else:
            self.connection = Redis(host=s.host, port=s.port, ssl=s.tls, decode_responses=True)

    def __getattr__(self, name):
        """
        I client ai servizi come Mongo e Redis vengono resi disponibili agli stage attraverso Manager
        di multiprocessing.managers, questo permette la comunicazione tra i processi workers nel caso
        di stage parallelizzabili
        I messaggi scambiati tra processi devono essere serializzabili, o 'pickable'
        Alcuni metodi della classe Redis ritornano tipi non pickable, per questo dobbiamo wrappare
        la chiamata e, nel caso sia necessario (come per gli Iterator e GeneratorType), trasformare
        l'oggetto ritornato in un tipo serializzabile
        """
        attr = getattr(self.connection, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, types.GeneratorType):
                    return list(result)
                else:
                    return result
            return wrapper

class MongoClient:
    def __init__(self, s:MongoSettings):
        self.connection = pymongo.MongoClient(host=s.host, port=s.port, tls=s.tls)

    def __getattr__(self, name):
        attr = getattr(self.connection, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, types.GeneratorType):
                    return list(result)
                else:
                    return result
            return wrapper

    def bulk_write(self, db, collection, ops):
        self.connection.get_database(db).get_collection(collection).bulk_write(ops)


class RedisClientProxy(BaseProxy):
    _exposed_ = set([attr for attr in dir(Redis) + dir(RedisCluster) + dir(RedisClient) if not attr.startswith("_")])
    
    def __getattr__(self, name):
        if not name.startswith("_"):
            return lambda *args, **kwargs: self._callmethod(name, args, kwargs)

class MongoClientProxy(BaseProxy):
    _exposed_ = set([attr for attr in dir(pymongo.MongoClient) + dir(MongoClient) if not attr.startswith("_")])

    def __getattr__(self, name):
        if not name.startswith("_"):
            return lambda *args, **kwargs: self._callmethod(name, args, kwargs)