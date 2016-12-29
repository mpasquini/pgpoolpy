import sys
import os
import logging
import py_pgpool

from argparse import SUPPRESS, ArgumentTypeError
from argh import ArghParser, arg, expects_obj, named


from py_pgpool.lockfile import GlobalCronLock as GCL, LockFileBusy, LockFilePermissionDenied

THIS_DIR = os.path.dirname(os.path.realpath(__file__))



def main():

    """
    The main method of py_pgpool
    """
    p = ArghParser(epilog='py_pgpool by LittleEaster (pasquini.matteo@gmail.com)')
    p.add_argument('-v', '--version', action='version',
                   version='%s\n\npy_pgpool LittleEaster (pasquini.matteo@gmail.com)'
                           % py_pgpool.__version__)
    p.add_argument('-c', '--config',
                   help='uses a configuration file '
                        '(defaults: %s)'
                        % ', '.join(py_pgpool.config.Config.CONFIG_FILES),
                   default=SUPPRESS)
    p.add_argument('-q', '--quiet', help='be quiet', action='store_true')
    p.add_argument('-d', '--debug', help='debug output', action='store_true')
    p.add_argument('-f', '--format', help='output format',
                   choices=output.AVAILABLE_WRITERS.keys(),
                   default=output.DEFAULT_WRITER)
    p.add_commands(
        [
            archive_wal,
            backup,
            check,
            cron,
            delete,
            diagnose,
            get_wal,
            list_backup,
            list_files,
            list_server,
            rebuild_xlogdb,
            receive_wal,
            recover,
            show_backup,
            show_server,
            replication_status,
            status,
            switch_xlog,
        ]
    )
    # noinspection PyBroadException
    try:
        p.dispatch(pre_call=global_config)
    except KeyboardInterrupt:
        msg = "Process interrupted by user (KeyboardInterrupt)"
        output.error(msg)
    except Exception as e:
        msg = "%s\nSee log file for more details." % e
        output.exception(msg)

    # cleanup output API and exit honoring output.error_occurred and
    # output.error_exit_code
    output.close_and_exit()







