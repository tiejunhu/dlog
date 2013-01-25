import logging
import threading
import sqlite3
import time
import socket
import struct

CRITICAL = 50
FATAL = CRITICAL
ERROR = 40
WARNING = 30
WARN = WARNING
INFO = 20
DEBUG = 10
NOTSET = 0

TCP_PORT = 3564

loggerDict = {}
loggerLock = threading.Lock()

def getLogger(name, host):
  loggerLock.acquire()
  try:
    if name not in loggerDict:
      loggerDict[name] = Logger(name, host)
    return loggerDict[name];
  finally:
    loggerLock.release()

class SqliteLogger:
  def __init__(self, name):
    self.conn = sqlite3.connect("%s.db" % (name))
    self._checkLogTable()

  def _hasLogTable(self):
    rows = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='log'")
    for row in rows:
      return True
    return False

  def _checkLogTable(self):
    if not self._hasLogTable():
      self.conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, level INTEGER, msg TEXT)")

  def log(self, level, msg):
    c = self.conn.cursor()
    c.execute("INSERT INTO log VALUES (null, strftime('%Y-%m-%d %H:%M:%f','now'), ?, ?)", (level, msg))
    self.conn.commit()
    c.close()

  def peekHead(self):
    c = self.conn.cursor()
    c.execute("SELECT * FROM log ORDER BY id LIMIT 1")
    head = c.fetchone()
    c.close()
    if head:
      return head
    else:
      return (0, '', 0, '')

  def removeLog(self, id):
    c = self.conn.cursor()
    c.execute("DELETE FROM log WHERE id = %d" % (id))
    self.conn.commit()
    c.close()

class SocketClient:
  def __init__(self, host):
    self._connect(host, TCP_PORT)
    self.host = host

  def send(self, entry):
    if not self.connected:
      self._connect(self.host, TCP_PORT)      
    try:
      self.target.send(self._pack(entry))
      return True
    except:
      self.connected = False
      return False

  def _pack(self, entry):
    result = ''
    for item in entry:
      length = len(str(item))
      result += struct.pack("< H %ds" % length, length, str(item))
    return result

  def _connect(self, host, port):
    try:
      (soc_family, _, _, _, address) = socket.getaddrinfo(host, port)[0]
      self.target = socket.socket(soc_family)
      self.target.connect(address)
      self.connected = True
    except:
      self.connected = False


class SendWorker(threading.Thread):
  def __init__(self, name, host):
    threading.Thread.__init__(self)
    self.name = name
    self.host = host
    self.socket = SocketClient(host)
    self.event = threading.Event()

  def run(self):
    sqlite = SqliteLogger(self.name)
    while True:
      entry = sqlite.peekHead()
      if entry[0]:
        if self.socket.send(entry):
          sqlite.removeLog(entry[0])
        else:
          time.sleep(2)
        time.sleep(0)
      else:
        self.event.clear()
        self.event.wait()

  def setEvent(self):
    self.event.set()

class Logger:
  def __init__(self, name, host):
    self.name = name
    self.level = WARN
    self._setupLogging()
    self.logger = logging.getLogger(name)
    self.sqlite = SqliteLogger(name)
    self.worker = SendWorker(name, host)
    self.worker.daemon = True
    self.worker.start()

  def setLevel(self, level):
    self.level = level

  def log(self, level, msg):
    if (level >= self.level):
      self.logger.log(level, msg)
      self.sqlite.log(level, msg)
      self.worker.setEvent()

  def _setupLogging(self):
      # logging.basicConfig(filename=logfile, level=logging.DEBUG, format='%(asctime)s - %(levelname)-8s %(name)s  %(message)s')
      logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)-8s %(name)s  %(message)s')
      # console = logging.StreamHandler()
      # console.setLevel(logging.DEBUG)
      # logging.getLogger('').addHandler(console)


def testPack():
  client = SocketClient('localhost')
  for i in client._pack([1, '2012-09-14 13:19:26,180', 30, 'message']):
    print "%d, %c" % (ord(i), i)

# logger = getLogger("test", "localhost")
# for i in range(100):
#   logger.log(WARN, "message")

testPack()

raw_input("Press Enter to continue...")