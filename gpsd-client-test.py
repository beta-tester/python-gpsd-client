#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: GPL-3.0-or-later
# Author: beta-tester


NAME = 'gpsd-client-test'
HOST_1 = '192.168.1.7'
HOST_2 = '192.168.1.8'

COMMIT_INTERVAL = 600 # [s]
OUTPUT_PATH = '.'
import tempfile
OUTPUT_PATH = tempfile.gettempdir()

#

TIMEOUT_STOP_WORKER_GPS = 60
TIMEOUT_STOP_WORKER_DATABASE = 600 # must be at least bigger than (TIMEOUT_STOP_WORKER_GPS + gps.gps() connection time out)

from threading import Thread, Event, Lock
from queue import Queue, Empty, Full
from time import time, monotonic, sleep
from datetime import datetime

import logging
import tempfile
LOGGER_LEVEL = logging.DEBUG
LOGGER_CONSOLE_LEVEL = logging.INFO
LOGGER_FILE_COUNT = 10
LOGGER_FILE_SIZE = 512 * 1024
LOGGER_OUTPUT_PATH = tempfile.gettempdir()

VERBOSE_GPS=0
#VERBOSE_GPS=5

DEBUG_DUPLICATES=True

NOW = datetime.utcnow().strftime("%Y-%m-%dT%H_%M_%SZ")

LIMIT=80

KEY_CUSTOM_HOST = '_h'
KEY_CUSTOM_TIME = '_t'
KEY_CUSTOM_FLAGS = '_f'
KEY_CUSTOM_EPOCH = '_e'
KEY_GPS_CLASS = 'class'
KEY_GPS_TIME = 'time'


TIMEOUT_QUEUE = 2
TIMEOUT_TRIGGER = TIMEOUT_QUEUE
TIMEOUT_GPS = 2
TIMEOUT_GPS_SOCKET = 5
TIMEOUT_DATABASE = 30
TIMEOUT_SCRIPT = 30
TIMEOUT_JOIN = 10
TIMEOUT_KEEP_ALIVE = 3


def print_DEBUG(s):
    #return
    try:
        print(s, end='', flush=True)
    except (BrokenPipeError, IOError) as ex:
        pass


class Stopper():
    __logger__ = None
    __stop__ = None
    __usr1__ = None
    __usr2__ = None

    def __init__(self, logger):
        super().__init__()
        Stopper.__logger__ = logger
        Stopper.__stop__ = Event()
        Stopper.__usr1__ = Event()
        Stopper.__usr2__ = Event()

        from atexit import register
        register(Stopper.__handle_exit_ATEXIT__)

        from signal import signal, Signals, SIGUSR1, SIGUSR2
        attach = {'SIGABRT', 'SIGBREAK', 'SIGHUP', 'SIGINT', 'SIGKILL', 'SIGTERM', 'CTRL_C_EVENT', 'CTRL_BREAK_EVENT', }
        handle = Stopper.__handle_stop__
        for sig in Signals:
            if sig.name in attach:
                try:
                    signal(sig, handle)
                    logger.debug(f"handling {sig} : {str(sig)}")
                except Exception as ex:
                    #logger.debug(f"failed {sig}")
                    pass
            else:
                #logger.debug(f"not handling {sig} : {str(sig)}")
                pass
        signal(SIGUSR1, Stopper.__handle_usr1__)
        signal(SIGUSR2, Stopper.__handle_usr2__)


    @property
    def stop_event(self):
        return Stopper.__stop__

    @property
    def usr1_event(self):
        return Stopper.__usr1__

    @property
    def usr2_event(self):
        return Stopper.__usr2__


    def is_stop_set(self):
        return Stopper.__stop__.is_set()

    def is_usr1_set(self):
        return Stopper.__usr1__.is_set()

    def is_usr2_set(self):
        return Stopper.__usr2__.is_set()


    @staticmethod
    def request_stop():
        try:
            logger = Stopper.__logger__
            if logger:
                from inspect import getframeinfo
                logger.debug(f"request_stop")
        except Exception:
            pass
        try:
            stop = Stopper.__stop__
            if stop:
                stop.set()
        except Exception:
            pass

    @staticmethod
    def __handle_exit_ATEXIT__():
        try:
            logger = Stopper.__logger__
            if logger:
                logger.debug("handle_exit_ATEXIT")
        except Exception:
            pass
        try:
            stop = Stopper.__stop__
            if stop:
                stop.set()
        except Exception:
            pass
        raise SystemExit

    @staticmethod
    def __handle_stop__(signum, frame):
        from signal import Signals, signal, SIG_DFL
        try:
            logger = Stopper.__logger__
            if logger:
                from inspect import getframeinfo
                logger.debug(f"handle_signal({signum} [{Signals(signum).name}], {getframeinfo(frame)})")
        except Exception:
            pass
        is_set = None
        try:
            stop = Stopper.__stop__
            if stop:
                is_set = stop.is_set()
                stop.set()
        except Exception:
            pass
        if is_set and Signals(signum).name in {'SIGINT', }:
            signal(signum, SIG_DFL)

    @staticmethod
    def __handle_usr1__(signum, frame):
        from signal import Signals
        try:
            logger = Stopper.__logger__
            if logger:
                from inspect import getframeinfo
                logger.debug(f"handle_signal({signum} [{Signals(signum).name}], {getframeinfo(frame)})")
        except Exception:
            pass
        try:
            event = Stopper.__usr1__
            if event:
                event.set()
        except Exception:
            pass

    @staticmethod
    def __handle_usr2__(signum, frame):
        from signal import Signals
        try:
            logger = Stopper.__logger__
            if logger:
                from inspect import getframeinfo
                logger.debug(f"handle_signal({signum} [{Signals(signum).name}], {getframeinfo(frame)})")
        except Exception:
            pass
        try:
            event = Stopper.__usr2__
            if event:
                event.set()
        except Exception:
            pass


class Logger(logging.getLoggerClass()):
    def __init__(self, name):
        super().__init__(name)
        import logging.handlers
        from os import path
        # create logger with name
        self.setLevel(LOGGER_LEVEL)
        # create file handler which logs even debug messages
        file_handler = logging.handlers.RotatingFileHandler(filename=path.join(LOGGER_OUTPUT_PATH, f"{name}_{NOW}.log"), maxBytes=LOGGER_FILE_SIZE, backupCount=LOGGER_FILE_COUNT)
        file_handler.setLevel(LOGGER_LEVEL)
        # create console handler with a higher log level
        console_handler = logging.StreamHandler()
        console_handler.setLevel(LOGGER_CONSOLE_LEVEL)
        # create formatter and add it to the handlers
        format='%(asctime)s %(name)-12s %(processName)-12s %(threadName)-12s %(levelname)-9s %(message)s'
        formatter = logging.Formatter(format)
        from time import gmtime
        formatter.converter = gmtime
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        # add the handlers to the logger
        self.addHandler(file_handler)
        self.addHandler(console_handler)
        # write for redirect stderr to logger debug
        self.write = lambda msg: self.debug(msg) if msg != '\n' else None


class WorkerDatabase(Thread):
    def __init__(self, *, name, logger, stop, queue, trigger):
        super().__init__()
        self.daemon = True
        self.name = name
        self.logger = logger
        self.stop = stop
        self.queue = queue
        self.trigger = trigger
        self.database = None

    def run(self):
        try:
            self.database = self.__setup_database__(NAME)
            debug_time_empty = None
            self.logger.info(f"waiting for data...")
            next_commit  = int(time())
            next_commit += COMMIT_INTERVAL - (next_commit % (COMMIT_INTERVAL))
            while not self.stop.is_set():
                print_DEBUG('.') # DEBUG: i am alive
                # take data from the queue
                try:
                    item = self.queue.get(timeout=TIMEOUT_QUEUE)
                    debug_time_empty = None
                except Empty:
                    item = None
                    debug_time = monotonic()
                    if not debug_time_empty:
                        debug_time_empty = debug_time
                        #self.logger.debug(f"empty queue")
                    else:
                        debug_time_delta = debug_time - debug_time_empty
                        if debug_time_delta > TIMEOUT_STOP_WORKER_DATABASE:
                            self.logger.error(f"detected empty queue for {debug_time_delta}s")
                            Stopper.request_stop() # DEBUG: GPS dead?
                except ValueError as ex:
                    self.logger.exception("queue closed", exc_info=ex)
                    break
                # put item data to database
                if item:
                    # put item to in-memory DB
                    self.__commit_in_memory__(item)
                    self.queue.task_done()
                current_time = int(time())
                if current_time > next_commit:
                    next_commit  = current_time
                    next_commit += COMMIT_INTERVAL - (next_commit % (COMMIT_INTERVAL))
                    # commit data from in-memory DB to file
                    self.__commit_file__()
                    # set trigger for external script
                    self.trigger.set()
        except Exception as ex:
            self.logger.exception(":", exc_info=ex)
            Stopper.request_stop() # in case of fatal error request stop for all
        finally:
            if self.database:
                self.__commit_file__()
                self.logger.debug("final commit done")
                self.database.close()
        self.logger.debug("done")

    def __setup_database__(self, name):
        """
        setup database (as file and in-memory)
        """
        import sqlite3
        from os import path
        db = None
        cur = None
        try:
            db = sqlite3.connect(path.join(OUTPUT_PATH, f"{name}.sqlite"), timeout=TIMEOUT_DATABASE)
            cur = db.cursor()
            cur.execute("ATTACH DATABASE 'file::memory:' AS mem;")
            cur.execute("PRAGMA journal_mode = 'WAL';")
            cur.execute("CREATE TABLE IF NOT EXISTS     data_gps_tpv (timestamp TEXT, host TEXT, mode INTEGER, lat REAL, lon REAL, alt REAL, e_lat REAL, e_lon REAL, e_alt REAL, track REAL, e_track REAL, speed REAL, e_speed REAL, climb REAL, e_climb REAL, PRIMARY KEY(timestamp, host));")
            cur.execute("CREATE TABLE IF NOT EXISTS mem.data_gps_tpv (timestamp TEXT, host TEXT, mode INTEGER, lat REAL, lon REAL, alt REAL, e_lat REAL, e_lon REAL, e_alt REAL, track REAL, e_track REAL, speed REAL, e_speed REAL, climb REAL, e_climb REAL, PRIMARY KEY(timestamp, host));")
            cur.execute("CREATE TABLE IF NOT EXISTS     data_gps_sky (timestamp TEXT, host TEXT, prn INTEGER, el INTEGER, az INTEGER, ss INTEGER, used INTEGER, PRIMARY KEY(timestamp, host, prn));")
            cur.execute("CREATE TABLE IF NOT EXISTS mem.data_gps_sky (timestamp TEXT, host TEXT, prn INTEGER, el INTEGER, az INTEGER, ss INTEGER, used INTEGER, PRIMARY KEY(timestamp, host, prn));")
            cur.execute("CREATE TABLE IF NOT EXISTS     data_gps_sky_map (host TEXT, el INTEGER, az INTEGER, ss INTEGER, used INTEGER, PRIMARY KEY (host, el, az));")
            cur.execute("CREATE TABLE IF NOT EXISTS mem.data_gps_sky_map (host TEXT, el INTEGER, az INTEGER, ss INTEGER, used INTEGER, PRIMARY KEY (host, el, az));")
            db.commit()
        except sqlite3.Error as ex:
            self.logger.exception("__setup_database__:", exc_info=ex)
        except sqlite3.OperationalError as ex:
            self.logger.exception("__setup_database__:", exc_info=ex)
        finally:
            if cur:
                cur.close()
        return db

    def __commit_in_memory__(self, item):
        """
        commit item data to in-memory database
        """
        from gps import (
                TIME_SET
                , TIMERR_SET
                , LATLON_SET
                , ALTITUDE_SET
                , SPEED_SET
                , TRACK_SET
                , CLIMB_SET
                , MODE_SET
                , HERR_SET
                , VERR_SET
                , SPEEDERR_SET
                , CLIMBERR_SET
                , DOP_SET
                , DEVICEID_SET
                )
        def get_finite(d, key):
            from math import isfinite
            value = d.get(key)
            if value and isfinite(value):
                return value
            return None
        def get_flagged(d, key, flags):
            key_flags = {
                # TPV
                'time' : TIME_SET,
                'ept' : TIMERR_SET,
                'lat' : LATLON_SET,
                'alt' : ALTITUDE_SET, # deprecated
                'altHAE' : ALTITUDE_SET,
                'altMSL' : ALTITUDE_SET,
                'speed' : SPEED_SET,
                'track' : TRACK_SET,
                'climb' : CLIMB_SET,
                'mode' : MODE_SET,
                'epx' : HERR_SET,
                'epy' : HERR_SET,
                'epv' : VERR_SET,
                'eps' : SPEEDERR_SET,
                'epc' : CLIMBERR_SET,
                # SKY
                'gdop' : DOP_SET,
                'hdop' : DOP_SET,
                'pdop' : DOP_SET,
                'tdop' : DOP_SET,
                'vdop' : DOP_SET,
                'xdop' : DOP_SET,
                'ydop' : DOP_SET,
                # DEVICE
                'driver' : DEVICEID_SET,
                'subtype' : DEVICEID_SET,
            }
            value = d.get(key)
            if value:
                flag = key_flags.get(key)
                if not flag or (flag and (flags & flag)):
                    return value
            return None
        #print_DEBUG(f"\n> '{item.get(KEY_CUSTOM_HOST)}' '{item.get(KEY_GPS_CLASS)}' {item.get(KEY_CUSTOM_TIME)} {item.get(KEY_CUSTOM_EPOCH)} {item.get(KEY_CUSTOM_FLAGS,0):X} #"[:LIMIT]) # DEBUG: item
        cur = None
        try:
            cls = item.get(KEY_GPS_CLASS)
            flags = item.get(KEY_CUSTOM_FLAGS, 0)
            host = item.get(KEY_CUSTOM_HOST)
            epoch = item.get(KEY_CUSTOM_EPOCH)
            if not epoch:
                epoch = item.get(KEY_GPS_TIME) # bug in gpsd <=v3.24 gps.read(), TIME_SET never set; get_flagged(item,'time', flags) won't work
                if not epoch:
                    epoch = item.get(KEY_CUSTOM_TIME)
            if cls == 'TPV':
                cur = self.database.cursor()
                cur.execute("""
                        INSERT INTO mem.data_gps_tpv (timestamp, host, mode, lat, lon, alt, e_lat, e_lon, e_alt, track, e_track, speed, e_speed, climb, e_climb)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT (timestamp, host) DO
                            UPDATE SET
                                lat = IFNULL(lat, EXCLUDED.lat), lon = IFNULL(lon, EXCLUDED.lon), alt = IFNULL(alt, EXCLUDED.alt)
                                , e_lat = IFNULL(e_lat, EXCLUDED.e_lat), e_lon = IFNULL(e_lon, EXCLUDED.e_lon), e_alt = IFNULL(e_alt, EXCLUDED.e_alt)
                                , track = IFNULL(track, EXCLUDED.track), e_track = IFNULL(e_track, EXCLUDED.e_track)
                                , speed = IFNULL(speed, EXCLUDED.speed), e_speed = IFNULL(e_speed, EXCLUDED.e_speed)
                                , climb = IFNULL(climb, EXCLUDED.climb), e_climb = IFNULL(e_climb, EXCLUDED.e_climb);
                        """
                        , (
                            epoch[:19], host
                            , get_flagged(item,'mode', flags)
                            , get_finite(item,'lat'), get_finite(item,'lon'), get_finite(item,'altHAE')
                            , get_finite(item,'epy'), get_finite(item,'epx'), get_finite(item,'epv')
                            , get_finite(item,'track'), get_finite(item,'ept')
                            , get_finite(item,'speed'), get_finite(item,'eps')
                            , get_finite(item,'climb'), get_finite(item,'epc')
                            )
                        )
                self.database.commit()
            elif cls == 'SKY':
                if 'satellites' not in item.keys():
                    return
                cur = self.database.cursor()
                for sat in item.get('satellites'):
                    cur.execute("""
                            INSERT OR IGNORE INTO mem.data_gps_sky (timestamp, host, prn, el, az, ss, used)
                            VALUES (?,?,?,?,?,?,?);
                            """
                            , (
                                epoch[:19], host
                                , sat.get('PRN')
                                , sat.get('el'), sat.get('az'), sat.get('ss')
                                , sat.get('used')
                                )
                            )
                    cur.execute("""
                            INSERT INTO mem.data_gps_sky_map (host, el, az, ss, used)
                            VALUES (?,?,?,?,?)
                            ON CONFLICT (host, el, az) DO
                                UPDATE SET
                                    ss = MAX(ss, EXCLUDED.ss)
                                    , used = MAX(used, EXCLUDED.used);
                            """
                            , (
                                host
                                , sat.get('el'), sat.get('az'), sat.get('ss')
                                , sat.get('used')
                                )
                            )
                self.database.commit()
        finally:
            if cur:
                cur.close()

    def __commit_file__(self):
        """
        commit in-memory data to database file
        """
        self.logger.info(f"commit in-memory DB to file...")
        cur = None
        try:
            cur = self.database.cursor()
            cur.execute("INSERT OR IGNORE INTO data_gps_tpv SELECT * FROM mem.data_gps_tpv;")
            cur.execute("INSERT OR IGNORE INTO data_gps_sky SELECT * FROM mem.data_gps_sky;")
            cur.execute("""
                    INSERT INTO data_gps_sky_map (host, el, az, ss, used)
                    SELECT host, el, az, ss, used
                    FROM mem.data_gps_sky_map
                    WHERE TRUE
                    ON CONFLICT (host, el, az) DO
                        UPDATE SET
                            ss = MAX(ss, EXCLUDED.ss)
                            , used = MAX(used, EXCLUDED.used);
                    """
                    )
            cur.execute("DELETE FROM mem.data_gps_tpv;")
            cur.execute("DELETE FROM mem.data_gps_sky;")
            cur.execute("DELETE FROM mem.data_gps_sky_map;")
            self.database.commit()
        finally:
            if cur:
                cur.close()
        self.logger.debug(f"...commit done")


class WorkerScript(Thread):
    def __init__(self, *, name, logger, stop, trigger):
        super().__init__()
        self.daemon = True
        self.name = name
        self.logger = logger
        self.stop = stop
        self.name = name
        self.trigger = trigger

    def run(self):
        """
        triggers external script
        """
        import subprocess
        try:
            self.logger.info(f"waiting for trigger...")
            while not self.stop.is_set():
                print_DEBUG(':') # DEBUG: i am alive
                try:
                    if self.trigger.wait(timeout=TIMEOUT_TRIGGER):
                        self.trigger.clear()
                        self.logger.debug(f"trigger script")
                        with subprocess.Popen(f"gnuplot -p {NAME}.gnuplot", shell=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as process:
                            try:
                                process.wait(timeout=TIMEOUT_SCRIPT)
                                if process.returncode:
                                    self.logger.warning(f"script returncode {process.returncode}")
                                else:
                                    self.logger.debug(f"script returncode {process.returncode}")
                            except TimeoutExpired as ex:
                                self.logger.exception(f"script timeout", exc_info=ex)
                                process.terminate()
                except Exception as ex:
                    self.logger.exception(f"Exception", exc_info=ex)
        finally:
            pass
        self.logger.debug(f"done")


class WorkerGps(Thread):
    def __init__(self, *, name, logger, stop, queue, host):
        super().__init__()
        self.daemon = True
        self.name = name
        self.logger = logger
        self.stop = stop
        self.queue = queue
        self.host = host
        self.session = None
        self.debug = None

    def run(self):
        def __heavy_copy__(value):
            """
            makes a heavy copy of the value
            transforms gps.client.dictwrapper to dict
            """
            from gps.client import dictwrapper
            if isinstance(value, dictwrapper) or isinstance(value, dict):
                ret_value = dict()
                for key in value:
                    ret_value[key] = __heavy_copy__(value[key])
            elif isinstance(value, list):
                ret_value = list()
                for sub_value in value:
                    ret_value.append(__heavy_copy__(sub_value))
            else:
                ret_value = value
            return ret_value
        from gps import (gps, WATCH_ENABLE, WATCH_NEWSTYLE, WATCH_JSON, WATCH_SCALED, WATCH_TIMING, PACKET_SET, SATELLITE_SET, TIME_SET, )
        import socket
        try:
            #socket.setdefaulttimeout(TIMEOUT_GPS_SOCKET)
            #self.session = gps(host=self.host, mode=WATCH_ENABLE|WATCH_JSON|WATCH_SCALED|WATCH_TIMING, reconnect=True, verbose=VERBOSE_GPS)
            self.session = gps(host=self.host, mode=WATCH_ENABLE|WATCH_NEWSTYLE, reconnect=True, verbose=VERBOSE_GPS)
            #self.session.sock.setblocking(False)
            self.session.sock.settimeout(TIMEOUT_GPS_SOCKET)
            debug_time_invalid = None
            debug_time_hash = None
            debug_hash_last = hash(None)
            debug_lb0=None
            debug_lb1=None
            debug_lb2=None
            debug_lb3=None
            debug_lb4=None
            debug_lb5=None
            debug_timeout=False
            epoch = datetime.fromtimestamp(0).isoformat()[:23]
            self.logger.info(f"waiting for data...")
            while not self.stop.is_set():
                print_DEBUG(f"{self.host[-1]}W") # DEBUG: i am alive
                # take session wrapper item from gps
                data_available = self.session.waiting(timeout=TIMEOUT_GPS)
                if data_available:
                    print_DEBUG(f"{self.host[-1]}wN") # DEBUG: i am alive
                    try:
                        self.session.valid = 0 # reset valid to see if we get a new item
                        self.session.data = None # reset valid to see if we get a new item
                        session_item = self.session.next()
                        session_valid = self.session.valid
                        session_response = self.session.response
                        if debug_timeout:
                            #self.logger.debug(f"{self.host}: just after TimeoutError,\n\twaiting() :{data_available},\n\tvalid     :0x{session_valid:X},\n\tbresponse :{self.session.bresponse},\n\tlinebuffer:{self.session.linebuffer},\n\tresponse  :{self.session.response},\n\tnext()    :{session_item},\n\tlinebuf t-1:{debug_lb1},\n\tlinebuf t-2:{debug_lb2},\n\tlinebuf t-3:{debug_lb3},\n\tlinebuf t-4:{debug_lb4},\n\tlinebuf t-5:{debug_lb5}")
                            pass
                        debug_timeout = False
                    except (TimeoutError, socket.timeout) as ex:
                        debug_timeout = True
                        session_item = None
                        session_valid = 0
                        session_response = None
                        #self.logger.exception(f"{self.host}: TimeoutError,\n\twaiting() :{data_available},\n\tvalid     :0x{session_valid:X},\n\tbresponse :{self.session.bresponse},\n\tlinebuffer:{self.session.linebuffer},\n\tresponse  :{self.session.response},\n\tnext()    :{session_item},\n\tlinebuf t-1:{debug_lb1},\n\tlinebuf t-2:{debug_lb2},\n\tlinebuf t-3:{debug_lb3},\n\tlinebuf t-4:{debug_lb4},\n\tlinebuf t-5:{debug_lb5}", exc_info=ex)
                        self.logger.debug(f"{self.host}: TimeoutError")

                    print_DEBUG(f"{self.host[-1]}n") # DEBUG: i am alive
                else:
                    print_DEBUG(f"{self.host[-1]}w") # DEBUG: i am alive
                    session_item = None
                    session_valid = 0
                    session_response = None


                debug_lb5 = debug_lb4
                debug_lb4 = debug_lb3
                debug_lb3 = debug_lb2
                debug_lb2 = debug_lb1
                debug_lb1 = debug_lb0
                debug_lb0 = self.session.linebuffer

                # reject invalid data
                debug_time = monotonic()
                if data_available and session_item and (PACKET_SET & session_valid):
                    debug_time_invalid = None
                    if DEBUG_DUPLICATES:
                        debug_hash = hash(self.session.response)
                        if debug_hash_last != debug_hash:
                            debug_hash_last = debug_hash
                            debug_time_hash = None
                        else:
                            #self.logger.debug(f"duplicate: hash:{debug_hash:X} hash_last:{debug_hash_last:X} {debug_hash_last != debug_hash}")
                            # detect nonstop abnormal same data
                            if not debug_time_hash:
                                debug_time_hash = debug_time
                                #self.logger.debug(f"duplicate:\n\twaiting() :{data_available},\n\tvalid     :0x{session_valid:X},\n\tbresponse :{self.session.bresponse},\n\tlinebuffer:{self.session.linebuffer},\n\tresponse  :{self.session.response},\n\tnext()    :{session_item},\n\tlinebuf t-1:{debug_lb1},\n\tlinebuf t-2:{debug_lb2},\n\tlinebuf t-3:{debug_lb3},\n\tlinebuf t-4:{debug_lb4},\n\tlinebuf t-5:{debug_lb5},\n\thash:{debug_hash:X}")
                            else:
                                debug_time_delta = debug_time - debug_time_hash
                                if debug_time_delta > TIMEOUT_STOP_WORKER_GPS:
                                    self.logger.error(f"detected nonstop duplicate for {debug_time_delta:.2f}s,\n\twaiting() :{data_available},\n\tvalid     :0x{session_valid:X},\n\tbresponse :{self.session.bresponse},\n\tlinebuffer:{self.session.linebuffer},\n\tresponse  :{self.session.response},\n\tnext()    :{session_item},\n\tlinebuf t-1:{debug_lb1},\n\tlinebuf t-2:{debug_lb2},\n\tlinebuf t-3:{debug_lb3},\n\tlinebuf t-4:{debug_lb4},\n\tlinebuf t-5:{debug_lb5},\n\thash:{debug_hash:X}")
                                    #self.debug = True
                                    break
                            print_DEBUG(f"{self.host[-1]}d-") # DEBUG: i am alive
                else:
                    # detect nonstop abnormal invalid data
                    if not debug_time_invalid:
                        debug_time_invalid = debug_time
                        #self.logger.debug(f"invalid:\n\twaiting() :{data_available},\n\tvalid     :0x{session_valid:X},\n\tbresponse :{self.session.bresponse},\n\tlinebuffer:{self.session.linebuffer},\n\tresponse  :{self.session.response},\n\tnext()    :{session_item},\n\tlinebuf t-1:{debug_lb1},\n\tlinebuf t-2:{debug_lb2},\n\tlinebuf t-3:{debug_lb3},\n\tlinebuf t-4:{debug_lb4},\n\tlinebuf t-5:{debug_lb5}")
                    else:
                        debug_time_delta = debug_time - debug_time_invalid
                        if debug_time_delta > TIMEOUT_STOP_WORKER_GPS:
                            self.logger.error(f"detected nonstop invalid for {debug_time_delta:.2f}s,\n\twaiting() :{data_available},\n\tvalid     :0x{session_valid:X},\n\tbresponse :{self.session.bresponse},\n\tlinebuffer:{self.session.linebuffer},\n\tresponse  :{self.session.response},\n\tnext()    :{session_item},\n\tlinebuf t-1:{debug_lb1},\n\tlinebuf t-2:{debug_lb2},\n\tlinebuf t-3:{debug_lb3},\n\tlinebuf t-4:{debug_lb4},\n\tlinebuf t-5:{debug_lb5}")
                            break
                    print_DEBUG(f"{self.host[-1]}i-") # DEBUG: i am alive
                    continue

                print_DEBUG(f"{self.host[-1]}|") # DEBUG: i am alive

                # handle only items of interest
                cls = session_item.get(KEY_GPS_CLASS)
                if cls in {'TPV', 'SKY', }:
                    if cls == 'SKY':
                        if 'satellites' not in session_item.keys():
                            session_item = None
                            continue
                    timestamp = datetime.utcnow().isoformat()[:23]
                    epoch = session_item.get(KEY_GPS_TIME, epoch)
                    #item = session_item
                    # take copy of session wrapper item for queue
                    item = __heavy_copy__(session_item)
                    # inject custom data
                    item[KEY_CUSTOM_FLAGS] = session_valid
                    item[KEY_CUSTOM_HOST] = self.host
                    item[KEY_CUSTOM_TIME] = timestamp
                    item[KEY_CUSTOM_EPOCH] = epoch
                    try:
                        print_DEBUG(f"\n< '{item.get(KEY_CUSTOM_HOST)}' '{item.get(KEY_GPS_CLASS)}' {item.get(KEY_CUSTOM_TIME)} {item.get(KEY_CUSTOM_EPOCH)} {item.get(KEY_CUSTOM_FLAGS,0):X} #"[:LIMIT]) # DEBUG: item
                        self.queue.put(item, timeout=TIMEOUT_QUEUE)
                    except Full as ex:
                        self.logger.exception(f"{self.host}: queue full", exc_info=ex)
                    except ValueError as ex:
                        self.logger.exception(f"{self.host}: queue closed", exc_info=ex)
                        break
                print_DEBUG(f"{self.host[-1]}u") # DEBUG: i am alive
                session_item = None
        except StopIteration as ex:
            self.logger.warning(f"{self.host}; StopIteration, GPSD has terminated")
        except ConnectionRefusedError as ex:
            self.logger.exception(f"{self.host}: ConnectionRefusedError", exc_info=ex)
            sleep(TIMEOUT_KEEP_ALIVE) # keep thread alive for some time - delays restart in main loop
        except OSError as ex:
            self.logger.exception(f"{self.host}: OSError", exc_info=ex)
        except Exception as ex:
            self.logger.exception(f"", exc_info=ex)
        finally:
            if self.session:
                self.session.close()
        self.logger.debug(f"{self.host}: done")


def main():
    logger = Logger(NAME)
    from sys import getswitchinterval, setswitchinterval
    logger.info(f"getswitchinterval: {getswitchinterval()}")
    from contextlib import redirect_stderr
    with redirect_stderr(logger):
        stop_db = Event()
        stopper = Stopper(logger)
        stop = stopper.stop_event
        queue = Queue(maxsize=1024)
        trigger = Event()
        workers_lock = Lock()
        with workers_lock:
            workers = [
                    WorkerDatabase(name="data", logger=logger, stop=stop_db, queue=queue, trigger=trigger)
                    , WorkerScript(name="script", logger=logger, stop=stop, trigger=trigger)
                    , WorkerGps(name="gps1", logger=logger, stop=stop, queue=queue, host=HOST_1)
                    , WorkerGps(name="gps2", logger=logger, stop=stop, queue=queue, host=HOST_2)
                    ]
            for worker in workers:
                worker.start()
        try:
            while not stop.wait(TIMEOUT_GPS):
                # restart gps workers, when they are not alive
                workers_lock.acquire(timeout=TIMEOUT_GPS)
                try:
                    for worker in tuple(workers):
                        if isinstance(worker, WorkerGps) and not worker.is_alive():
                            if worker.debug:
                                stop.set() # DEBUG:
                            elif not stop.is_set():
                                workers.remove(worker)
                                logger.info(f"restart worker '{worker.name}'")
                                w = WorkerGps(name=worker.name, logger=logger, stop=stop, queue=queue, host=worker.host)
                                w.start()
                                workers.append(w)
                finally:
                    workers_lock.release()
            logger.info("stop requested")
        except KeyboardInterrupt:
            logger.debug("KeyboardInterrupt")
        except SystemExit:
            logger.debug("SystemExit")
        except Exception as ex:
            logger.exception("unhandled exception", exc_info=ex)
        finally:
            # signal stop workers
            logger.debug(f"signal stop workers")
            stop.set()
            # wait for empty queue
            logger.debug(f"wait for empty queue")
            queue.join()
            # signal stop data workers
            logger.debug(f"signal stop data worker")
            stop_db.set()
            # wait for workers
            with workers_lock:
                for worker in reversed(workers):
                    logger.debug(f"wait for worker '{worker.name}'")
                    worker.join(TIMEOUT_JOIN)
                    if worker.is_alive():
                        logger.warning(f"worker '{worker.name}' is still alive")
                    else:
                        logger.debug(f"worker '{worker.name}' done")
            logger.info("done")


if __name__ == '__main__':
    main()
