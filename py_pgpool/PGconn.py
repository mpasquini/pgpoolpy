"""
- sceglie il server in base al delay
- * offre una e una sola connessione read write
- * offre un pooler di connessioni read only
- offre connessioni ro tramite un pooler la cui size deve essere allineata al maxthreads
- recupera una tabella/query per la lista delle tiles ola singola PnP
- #TODO PNP: recupera tabelle specifiche in base alla configurazione, resituisce liste, namedtuple, dizionari   -> pooler

"""
import os, re
from Queue import Queue
from threading import Thread
from time import sleep
import prctl
import psycopg2
from psycopg2.extras import NamedTupleCursor
from psycopg2.pool import ThreadedConnectionPool as TCP
from psycopg2 import OperationalError, errorcodes, Error
from psycopg2.extensions import TransactionRollbackError, QueryCanceledError

from py_pgpool import CONSTRAINT, config
from errors import *
import logging
from utils import DoCmd

# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# TODO add logger
cfg = config.ConfigPmo()

logger_app = logging.getLogger('app')
logger_nps = logging.getLogger('nps')

NAPTIME = CONSTRAINT.PGCONN_NAPTIME



class PgConn(object):
    """
    Init this after ConfigPmo init (else thow error)
    choose a unique key and get a read only connection using get_ro_conn(key=)
        limit calls to max number of thread set
        if the pooler is full it blocks until a connection is freed
    set back the read only connection with put_ro_conn(connectio, key)

    get the list of tiles or the PnP using get_rowdata(sql) where sql is one of:
        ConfigPmo().sql_get_tiles -> that give a list of rows
        ConfigPmo().sql_get_pnp % (..list of consumed oid..) -> gie a single tile

    Write connection are set but not implemented diretcly here, use PmoDownloadTs()

    """
    _instance = None    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(PgConn, cls).__new__(cls, *args)
            # cls.instance = super.__new__(cls) # Py3.5
        return cls._instance

    def __init__(self):
        # TODO add case when there is no need of a write connection, e.g. PnP case
        self.ro_conn_str = cfg.db_ns_connstr
        self.rw_conn_str = cfg.db_nt_connstr
        if not self.check_dsn(self.ro_conn_str):
            logger_app.error('host is not reachable %s' % self.ro_conn_str)
            raise Exception('host is not reachable %s' % self.ro_conn_str)
        if not self.check_dsn(self.rw_conn_str):
            logger_app.error('host is not reachable %s' % self.rw_conn_str)
            raise Exception('host is not reachable %s' % self.rw_conn_str)

        self.threads = cfg.app_max_threads if cfg.app_max_threads > 1 else 1

        self._ro_pooler = None
        self._rw_conn = None
        self._init_connections()

    def check_dsn(self, dsn):
        host_parser = re.compile('^(?P<has_scheme>(?P<scheme>(http|ftp)s?)://)?(?P<host>(?P<boh2>[A-Z0-9](?P<boh3>[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?P<domain>[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|localhost|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(?P<port>:\d+)?(?P<path>/?|[/?]\S+)$', re.IGNORECASE)
        match = None
        for par in dsn.split(' '):
            if 'host' in par.lower():
                match = host_parser.match(par.split('=')[1])
                break
        if self._ping(match.groupdict()['host']):
            return True
        else:
            return False

    def _ping(self, hostname):
        code, stout, err = DoCmd("ping -c 5 " + hostname)
        # response = os.system("ping -c 1 " + hostname)
        return not bool(code) # success is 0

    def _init_connections(self):
        # TODO missing case where there is no write connection
        """check connections and check if the ro connection is on a slave, check the delay threshold and decide what
        to do on switchondelay2nt parametner
        check rw connection is on master
        throw error on
        -ro slave connection is on delay and cannot switch on master
        -rw connection string not point to amaster database"""
        ro = self.ro_conn_str
        rw = self.rw_conn_str
        c = psycopg2.connect(ro)
        cres = self.getDelayStatus(c)
        c.close()
        del c
        if cres[0] and cres[1] <= cfg.sync_delay_limit:
            self.ro_conn_str = ro
            self._ro_pooler = TCP(2, self.threads + 3, self.ro_conn_str)
        elif cres[0] and cres[1] > cfg.sync_delay_limit and cfg.sync_switchondelay2nt:
            self.ro_conn_str = rw
            self._ro_pooler = TCP(2, self.threads + 3, self.ro_conn_str)
        elif cres[0] and cres[1] > cfg.sync_delay_limit and not cfg.sync_switchondelay2nt:
            logger_app.error(Exception('Sync status exceed paramenters, configured to NOT switch to NT. aborting') )
            exit()

        c = psycopg2.connect(rw)
        cres = self.getDelayStatus(c)
        c.close()
        del c
        if not cres[0]:
            self.rw_conn_str = rw
            self._rw_conn = psycopg2.connect(self.rw_conn_str)
        else:
            raise Exception('Write connection not pointing to Master database, aborting')

    def isSlave(self, conn):
        """duplicated in base class, USE THAT ONE"""
        cursor = conn.cursor()
        cursor.execute('select pg_is_in_recovery()')
        ret = bool(cursor.fetchone()[0])
        cursor.close()
        return ret

    def getDelay(self, conn):
        """returns None in case there is no streaming replication or is executed on master, so self.isSlave have to be
        False. It not check the status of the master when executed on slave"""
        cursor = conn.cursor()
        cursor.execute(
            r'SELECT CASE WHEN pg_last_xlog_receive_location() = pg_last_xlog_replay_location() THEN 0 ELSE EXTRACT ('
            r'EPOCH FROM now() - pg_last_xact_replay_timestamp())::INTEGER END')
        ret = cursor.fetchone()[0]
        cursor.close()
        return ret

    def getDelayStatus(self, conn):
        return self.isSlave(conn), self.getDelay(conn)

    def get_rowdata(self, sql, *args):
        """to be called from main PMOTiles.py
        used fot Tiles, SSC, S57.  NOT for PnP"""
        key = 'get_rowdata'
        ret = []
        try:
            conn = self._ro_pooler.getconn(key=key)
            cur = conn.cursor(cursor_factory=NamedTupleCursor)
            # cur.execute(cfg.sql_get_tiles) << example
            logger_app.debug(cur.mogrify(sql, args))
            cur.execute(sql, args)
            ret = cur.fetchall()
            conn.rollback()
            cur.close()
        except TransactionRollbackError as e:
            logger_nps.error('while: %s\n pgerror %s\n pgmessage: %s \n on %s' % (key, e.message, e.pgerror, args))
        except Error as e:
            logger_nps.error('while: %s\n pgerror %s\n pgmessage: %s \n on %s' % (key, e.message, e.pgerror, args))
        except Exception as e:
            logger_nps.error('while: %s\n pgerror %s' % (key, e.message))
        finally:
            self._ro_pooler.putconn(conn, key=key, close=False)
        return ret

    def read(self, sql='select 1', *args, **kwargs):
        conn = self._ro_pooler.getconn()
        cur = conn.cursor()
        cur.execute('select 1')
        ret = cur.fetchall()
        cur.close()
        conn.close()
        return ret

    def write(self, args):
        """ the unique write should be the PmoDonwloadTs for Tiles, PnP uses local shelve fd
        args should be the sql from configuration fd"""
        assert NotImplemented

    def __close__reader(self):
        if not self._ro_pooler is None:
            self._ro_pooler.closeall()

    def __close__writer(self):
        if not self._rw_conn is None:
            self._rw_conn.close()

    def close(self):
        logger_app.debug('closing pg connections...')
        self.__close__writer()
        self.__close__reader()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_ro_conn(self, key):
        if key in self._ro_pooler._used.keys():
            raise Exception('key %s alredy in use' % key)
        pooled_connections = self._ro_pooler._used.keys().__len__()
        available_connections = self._ro_pooler.maxconn - pooled_connections
        while available_connections < 1:
            logger_app.debug('insufficent connection pools pooled_connections: %s, available_connections: %s' %(
                pooled_connections, available_connections) )
            sleep(NAPTIME)
        c = False
        while not c:
            try:
                c = self._ro_pooler.getconn(key=key)
            except:
                # TODO log debug it
                sleep(NAPTIME)
        return c

    def put_ro_conn(self, conn=None, key=None):
        if self._ro_pooler._used.has_key(key):
            self._ro_pooler.putconn(conn=conn, key=key, close=False)
        else:
            self._ro_pooler.putconn(conn=conn, close=False)

    @property
    def get_rw_conn(self):
        """ if the connection is required more than one time the session will be shared, eyes on commits and 
        rollbacks..."""
        return self._rw_conn

class PmoDownloadTs(PgConn):
    """
    initiate PgConn (singleton)
    initiate PmoDonwloadTs (singleton)
    use q_in.put(oid) on PmoDonwloadTs instance
    """
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(PmoDownloadTs, cls).__new__(cls, *args, **kwargs)
            # cls.instance = super.__new__(cls) # Py3.5
        return cls._instance
    
    def __init__(self):
        PgConn.__init__(self)
        self.q_in = Queue()
        # self.cursor = self._rw_conn.cursor()
        self._set_thread()

    def _set_thread(self):
        self.w = Thread(target=self._set_pmo_dw_ts)
        # self.w.setDaemon(True)
        self.w.start()

    def _set_pmo_dw_ts(self):
        prctl.prctl(prctl.NAME, 'PMOpg_')
        while True:
            size, id = self.q_in.get(timeout=3)
            if size is not None:
                print 'pg :putting'
                prctl.prctl(prctl.NAME, 'PMOpg:' + str(id))
                self.cursor = self._rw_conn.cursor()
                if cfg.app_debug_only_pmodownload_ts:
                    logger_app.debug(self.cursor.mogrify(cfg.sql_set_download_flag % (size, '%s'), (id,)))

                if cfg.app_debug_only_pmodownload_ts:
                    logger_nps.info('debug_only_pmodownload_ts is Ture, omitting to set pmo_downloaded_ts for: %s' % id)
                else:
                    self.cursor.execute(cfg.sql_set_download_flag % (size, '%s'), (id,))
                    self._rw_conn.commit() # TODO modify before PRODUCTION
                    logger_nps.info('pmo_downloaded_ts set for %s' % id)

                self.cursor.close()
                self.q_in.task_done()
        return

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connections()

    def close_connections(self):
        logger_app.debug('pgconn joining queue')
        self.q_in.join()
        # self.w.join()
        logger_app.debug('self.w_runner.join()')
        self.close()
        logger_app.debug('PgConn.close()')


class PgBuilder(object):
    PROFILES = {True: PmoDownloadTs, False: PgConn}
    @classmethod
    def build(cls):
        pg = cls.PROFILES[cfg.app_set_pmodownload_ts]()
        return pg

#
# if __name__ == '__main__':
#     pgconn = PgConnector().build()
#










