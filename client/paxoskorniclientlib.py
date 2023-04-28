import asyncio, hashlib, uuid, json, sqlite3
import subprocess
from subprocess import PIPE


# all keys split to 4096 partitions -> 4096 each kind tables in container
from time import sleep


def partitionFromRegKey(regKey: str) -> str:
    return hashlib.sha256(regKey.encode('utf-8')).hexdigest()[:3].upper()
def partitionFromIndex(index: int):
    if index < 0 or index > 4095:
        raise Exception('rtition index must be in [0..4095] interval')
    return str(hex(index)).upper()[2:]

class CASPaxosClient(object):
    # make REQUEST FOR CHANGE
    @staticmethod
    def paxosKorni3ClientRequest(key: str, function_id: str, nextValue, korni3container='paxos'):
        """ return (requestId: str | None, asincio  Future | None )"""

        # OLD """ return (requestId: str | None, asincio  Future | None, asincio Task for check result | None )"""
        table = 'PAXOS-CHANGE-REQ-Part-'+partitionFromRegKey(key)
        id = str(uuid.uuid4())
        data = {"id": id, "reg": key, "fId": function_id, "next": nextValue, "handled": False}

        def working(i):
            p = subprocess.run(['korni3', 'insert', korni3container, table], input=json.dumps(data).encode('utf-8'))
            if p.returncode == 1:
                print('ERROR run korni3 ', p.stderr.decode('utf-8'), p.stdout.decode('utf-8'))
            return p.returncode
        ret = 100
        for i in range(0, 10):
            if ret == 100:
                ret = working(i)
            if ret == 100:
                sleep(3)
                print('need repeat - db locked ....')
                ret = working(i)
        if ret == 1:
            return None, None
        else:
            future = asyncio.Future()

            paxosKorni3ClientCheckResult = CASPaxosClient._paxosKorni3GetChecker('paxos')
            async def task_check():
                while True:
                    v = paxosKorni3ClientCheckResult(id, key)
                    if not v is None:
                        print('v=', v)
                        future.set_result(v)
                        task.cancel()
                    await asyncio.sleep(3) # check every 3 sec TODO make it a param

            task = asyncio.create_task(task_check())
            return id, future

    # return function for check values
    @staticmethod
    def _paxosKorni3GetChecker(containerName: str):
        def working(i):
            r = subprocess.run(["korni3", "db", containerName], stdout=PIPE, stderr=PIPE)
            return r.returncode, r
        ret = 100
        r = None
        for i in range(0, 10):
            if ret == 100:
                ret, r = working(i)
            if ret == 100:
                sleep(3)
                print('need repeat - db locked ....')
                ret, r = working(i)
        if ret != 0:
            print('ERROR: make several atemts but can not connect to DB')
            exit(100)
        else:
            path = r.stdout.decode('utf-8').strip()

            def createIndex(name, table, createquery):
                try:
                    con = sqlite3.connect(path, timeout=5)
                    cur = con.cursor()
                    try:
                        cur.execute('SELECT * FROM sqlite_master WHERE type="index" and tbl_name=? and name=? ',
                                         (table, name))
                        if not cur.fetchone():
                            cur.execute(createquery)
                            cur.connection.commit()
                    except sqlite3.OperationalError:
                        pass
                finally:
                    con.close()
            # TODO ? откуда клиент узнает в какой таблице ожидать результат? какую таблицу ему нужно загружать для проверки ?
            # он конечно может вычислить (просто вызвать функцию), но пока что ему апи напрямую это не говорит
            def paxosKorni3ClientCheckResult(reqid, key):
                try:
                    partitionId = partitionFromRegKey(key)
                    createIndex("result-id-reg-" + partitionId, "PAXOS-RESULT-Part-" + partitionId,
                                'CREATE INDEX "result-id-reg-' + partitionId +
                                '" ON "PAXOS-RESULT-Part-' + partitionId + '" ( "id", 	"reg" );')
                    createIndex("result-id-zT-" + partitionId, "PAXOS-RESULT-Part-" + partitionId,
                                'CREATE INDEX "result-id-zT-' + partitionId +
                                '" ON "PAXOS-RESULT-Part-' + partitionId + '" (  "id", 	"zT" );')

                    con = sqlite3.connect(path, timeout=5)
                    cur = con.cursor()

                    try:
                        cur.execute('select state, id, zT from "PAXOS-RESULT-Part-'+partitionId+'" where id=? and reg=?',
                                    (reqid, key) )
                        try:
                            r = cur.fetchone()
                            val = json.loads(r[0]) # 'CONFLICT' - is not value

                            # mark as handled
                            cur.execute('update "PAXOS-RESULT-Part-'+partitionId+'" set handled=1 where id=? and zT=?', (r[1], r[2]))
                            con.commit()
                            return val if val!='CONFLICT' else None # TODO ? сделать чтобы значение могло быть CONFLICT. надо просто больше тестировать и доулучшить
                        except TypeError:
                            return None
                    except sqlite3.OperationalError:
                        return None
                finally:
                    con.close()
            return paxosKorni3ClientCheckResult

