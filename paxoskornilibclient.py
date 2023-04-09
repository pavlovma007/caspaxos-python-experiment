import asyncio, hashlib, uuid, json, sqlite3
import subprocess
from subprocess import PIPE


# all keys split to 4096 partitions -> 4096 each kind tables in container
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
        p = subprocess.run(['korni3', 'insert', korni3container, table], input=json.dumps(data).encode('utf-8'))
        if p.returncode != 0:
            print('ERROR run korni3 ', p.stderr.decode('utf-8'), p.stdout.decode('utf-8'))
            return None,None #,None
        else:
            future = asyncio.Future()

            paxosKorni3ClientCheckResult = CASPaxosClient.paxosKorni3GetChecker('paxos')
            async def task_check():
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
    def paxosKorni3GetChecker(containerName: str):
        r = subprocess.run(["korni3", "db", containerName], stdout=PIPE, stderr=PIPE)
        path = r.stdout.decode('utf-8').strip()
        con = sqlite3.connect(path)
        cur = con.cursor()
        def createIndex(name, table, createquery):
            try:
                cur.execute('SELECT * FROM sqlite_master WHERE type="index" and tbl_name=? and name=? ',
                                 (table, name))
                if not cur.fetchone():
                    cur.execute(createquery)
            except sqlite3.OperationalError:
                pass
        # TODO ? откуда клиент узнает в какой таблице ожидать результат? какую таблицу ему нужно загружать для проверки ?
        # он конечно может вычислить (просто вызвать функцию), но пока что ему апи напрямую это не говорит
        def paxosKorni3ClientCheckResult(reqid, key):
            # --
            partitionId = partitionFromRegKey(key)
            createIndex("result-id-reg-" + partitionId, "PAXOS-RESULT-Part-" + partitionId,
                        'CREATE INDEX "result-id-reg-' + partitionId +
                        '" ON "PAXOS-RESULT-Part-' + partitionId + '" ( "id", 	"reg" );')
            createIndex("result-id-zT-" + partitionId, "PAXOS-RESULT-Part-" + partitionId,
                        'CREATE INDEX "result-id-zT-' + partitionId +
                        '" ON "PAXOS-RESULT-Part-' + partitionId + '" (  "id", 	"zT" );')
            try:
                cur.execute('select state, id, zT from "PAXOS-RESULT-Part-'+partitionId+'" where id=? and reg=?',
                            (reqid, key) )
                try:
                    r = cur.fetchone()
                    val = json.loads(r[0]) # 'CONFLICT' - is not value

                    # f = CASPaxosClient.m_requests[(reqid, key)]
                    # if f:
                    #     f.set_result(val)

                    # mark as handled
                    cur.execute('update "PAXOS-RESULT-Part-'+partitionId+'" set handled=1 where id=? and zT=?', (r[1], r[2]))
                    con.commit()
                    return val if val!='CONFLICT' else None # TODO ? сделать чтобы значение могло быть CONFLICT. надо просто больше тестировать и доулучшить
                except TypeError:
                    return None
            except sqlite3.OperationalError:
                return None
        return paxosKorni3ClientCheckResult

