import sys
import traceback
import zipfile
from datetime import timedelta, datetime, tzinfo
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
import os
from os import unlink
from os.path import basename, isfile
from smtplib import SMTP
from dateutil.parser import parse
import hashlib
import signal
from subprocess import PIPE, Popen
from time import sleep


import cStringIO as StringIO



class GracefulInterruptHandler(object):
    def __init__(self, sig=signal.SIGTERM):
        self.sig = sig

    def __enter__(self):
        self.interrupted = False
        self.released = False

        self.original_handler = signal.getsignal(self.sig)

        def handler(signum, frame):
            self.release()
            self.interrupted = True
            print('handling....')
        signal.signal(self.sig, handler)

        return self

    def __exit__(self, type, value, tb):
        self.release()

    def release(self):
        print('releasing...')
        if self.released:
            return False

        signal.signal(self.sig, self.original_handler)

        self.released = True

        return True


def get_md5(oo):
    if isinstance(oo, StringIO.StringIO):
        oo.seek(0)
        ret = hashlib.md5(oo.read(-1)).hexdigest()
        oo.seek(0)

    if isinstance(oo, str):
        ret = hashlib.md5(oo).hexdigest()

    return ret

def DoCmd(cmd):
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    (stdoutdata, stderrdata) = p.communicate()
    p.wait()
    # if p.returncode != 0:
    #     pass
    # p.communicate(input=cmd)
    return p.returncode, stdoutdata, stderrdata

def WriteCmd(cmd, file, app_log_ps_frequency):
    while True:
        file.write(DoCmd(cmd))
        sleep(app_log_ps_frequency)

def ps_collect_mem_usage(THIS_DIR, app_log_ps_frequency):
    """
    http://ss64.com/bash/ps.html
       PROCESS STATE CODES
   D   uninterruptible sleep (usually IO)
   R   runnable (on process queue)

   S   sleeping
   T   traced or stopped
   Z   a defunct ("zombie") process

   For BSD formats and when the "stat" keyword is used,
   additional letters can be displayed:
   W   has no resident pages
   <   high-priority process
   N   low-priority task
   L   has pages locked into memory (for real-time and custom IO)

    ps -eo pid,%cpu,%mem,pmem,stat,time,tt
    ps -eo pid,%cpu,%mem,pmem,stat,time,tt,session,ppid  --no-headers
    used : ps -o pid,pcpu,pmem,stat,etime,rsz,vsz,size,tt --no-headers $(cat .cron.lock)
    header example:
      PID %CPU %MEM STAT     ELAPSED   RSZ    VSZ  SIZE TT
     6777  0.1  0.7 Sl+        08:52 23984 579552 455864 pts/4
    :return:
    """

    from os.path import join, isfile
    from os import getpid
    from threading import Thread


    PID = getpid()
    # cmd = 'ps -HSm  %s ' % PID
    cmd = 'ps -o pid,pcpu,pmem,stat,etime,rsz,vsz,size,tt %s ' % PID
    psfile = join(THIS_DIR, 'log', 'ps_log_' + datetime.today().strftime('%Y_%m_%d_%H_%M'))
    if isfile(psfile):
        unlink(psfile)
    ps_file = open(psfile, 'a')
    ps_file.write(DoCmd(cmd)[1])
    # cmd = 'ps -HShm  %s ' % PID
    cmd = 'ps -o pid,pcpu,pmem,stat,etime,rsz,vsz,size,tt --no-headers %s ' % PID

    psworker = Thread(target=WriteCmd, args=(cmd, ps_file, app_log_ps_frequency), name='ps_logger')
    psworker.setDaemon(True)
    psworker.start()


# unused
def ZipFile(fil):
    ret = False
    archieve = fil[:-3] + 'zip'
    if isfile(archieve):
        unlink(archieve)
    zf = zipfile.ZipFile(archieve, mode='w_starter', compression=zipfile.ZIP_DEFLATED)
    try:
        zf.write(fil, basename(fil))
        ret = archieve
    except:
        ret = 'failed to build archieve %s ' % archieve
    finally:
        zf.close()
    return ret


def ZipFiles(filList, archieve):
    ret = False
    if isfile(archieve):
        unlink(archieve)
    zf = zipfile.ZipFile(archieve, mode='a', compression=zipfile.ZIP_DEFLATED)
    try:
        for fil in filList:
            zf.write(fil, basename(fil))
        ret = archieve
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        errl = traceback.format_exception(exc_type, exc_value, exc_traceback, limit=None)
        print errl
        ret = 0
    finally:
        zf.close()
    return ret


# @staticmethod
def update_progress(progress):
    sys.stdout.write('\r[%s] %s%%' % ('#' * progress, progress))
    # sys.stdout.write('\r[%s] %s%%' % ('#' * (progress / 10), progress))
    sys.stdout.flush()



def log_traceback_error(alogger, *args):
    herr = '_______________ERROR________________\n'
    # err = '\n'.join(args)
    alogger.error(args)


def mkdirs(fullpath):
    drive = os.path.splitdrive(fullpath) # '' on linux
    dirs = fullpath.split(os.path.sep)[1:]
    adir = drive[0] + '\\' if drive[0] != '' else '/' # '' to not import also platform ...
    for d in dirs:
        if d != '':
            adir = os.path.join(adir, d)
            if not os.path.isdir(adir):
                try:
                    os.mkdir(adir)
                except:
                    pass


class UTC(tzinfo):
    def utcoffset(self, dt):
        return timedelta(hours=0) + self.dst(dt)
    def dst(self, dt):
            return timedelta(0)
    def tzname(self,dt):
         return "UTC"


def convertTimestampToFloat(ts):
    tz = UTC()
    _EPOCH = datetime(1970, 1, 1, tzinfo=tz)
    ts = parse(ts)
    return (ts.replace(tzinfo=tz) - _EPOCH).total_seconds()








