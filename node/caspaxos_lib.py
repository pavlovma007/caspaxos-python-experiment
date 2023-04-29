import hashlib, json, sqlite3, subprocess, time
import os
from subprocess import PIPE
from change_functions import getFunction
from time import sleep

def partitionFromIndex(index: int):
    if index < 0 or index > 4095:
        raise Exception('rtition index must be in [0..4095] interval')
    return str(hex(index)).upper()[2:]
def partitionFromRegKey(regKey: str) -> str:
    return hashlib.sha256(regKey.encode('utf-8')).hexdigest()[:3].upper()

class AcceptorKorni3(object) :
    #partitionId # TODO clear
    #tableName data table name
    #zU public key of user for control comunication

    # promise = 0  # ballot number
    # accepted = (0, 0)

    #korni3container : str container name
    #cur : SQlite3 cursor

    # def isMinePertition(self, partitionId):
    #     return partitionId in self.partitions
    def preqtable(self, partition):
        return 'REQ-PREPARE-Part-' + partition
    def presptable(self, partition):
        return 'RESP-PREPARE-Part-' + partition
    def areqtable(self, partition):
        return 'REQ-ACCEPT-Part-' + partition
    def aresptable(self, partition):
        return 'RESP-ACCEPT-Part-' + partition
    def tableName(self, partition):
        return 'PaxosAcceptor'+'-' + partition
    def __init__(self, partitions, korni3container: str, zU: str):
        self.partitions = partitions
        self.mePubKey = zU

        self.korni3container = korni3container
        r = subprocess.run(["korni3", "db", self.korni3container], stdout=PIPE, stderr=PIPE)
        self.path = r.stdout.decode('utf-8').strip()

        try:
            cur, con = self.cur()
            def createIndex(name, table, createquery):
                try:
                    cur.execute('SELECT * FROM sqlite_master WHERE type="index" and tbl_name=? and name=? ',
                                     (table, name))
                    if not cur.fetchone():
                        cur.execute(createquery)
                except sqlite3.OperationalError: pass
            for partitionId in self.partitions:
                createIndex("preq-aid-handled-"+partitionId, "REQ-PREPARE-Part-"+partitionId,
                            'CREATE INDEX "preq-aid-handled-'+partitionId+
                            '" ON "REQ-PREPARE-Part-'+partitionId+'" ( 	"aid", 	"handled" );')

                createIndex("preq-handled-id-zT-"+partitionId , "REQ-PREPARE-Part-"+partitionId,
                            'CREATE INDEX "preq-handled-id-zT-'+partitionId+'" ON "REQ-PREPARE-Part-'
                            +partitionId+'" ( 	"handled", 	"id", 	"zT" );')

                createIndex("areq-aid-handled-"+partitionId, "REQ-ACCEPT-Part-"+partitionId,
                            'CREATE INDEX "areq-aid-handled-'+partitionId+
                            '" ON "REQ-ACCEPT-Part-'+partitionId+'" (	"aid",	"handled");')

                createIndex("areq-handled-aid-zT-"+partitionId, "REQ-ACCEPT-Part-"+partitionId,
                            'CREATE INDEX "areq-handled-aid-zT-'+partitionId+
                            '" ON "REQ-ACCEPT-Part-'+partitionId+'" (	"handled", 	"aid", 	"zT" );')
            cur.connection.commit()
        finally:
            con.close()


    def _write(self, register: str, promise, accepted): # return void
        if not self._validate(register): return None

        data = {"id": register, "promise": promise, "accepted": accepted}
        # , '--ignore'

        def working(i):
            p1 = subprocess.run(
                ['korni3', 'insert', self.korni3container, self.tableName(partitionFromRegKey(register))],
                input=json.dumps(data).encode('utf-8'), stdout=PIPE, stderr=PIPE)
            if p1.returncode == 1:
                print('ERROR korni3 acceptor _write', p1.stdout.decode('utf-8'), p1.stderr.decode('utf-8'))
            return p1.returncode
        ret = 100
        for i in range(0, 10):
            if ret == 100 :
                ret = working(i)
            if ret == 100:
                sleep(3)
                print('need repeat - db locked ....')
                ret = working(i)

    def cur(self): # can be None
        con = sqlite3.connect(self.path, timeout=5)
        return con.cursor() , con

    def _read(self, registerKey: str) :  # return tuple ( promise, accepted: (0,0) )
        try:
            cur, con = self.cur()
            if not self._validate(registerKey): return None
            defaultVal = (0, (0,0))
            try:
                cur.execute('select promise, accepted from "' + self.tableName(partitionFromRegKey(registerKey))
                            +'"  where id=? and zU=?   ORDER by zT DESC limit 1',  (registerKey, self.mePubKey))
                result = cur.fetchone()

                if result is not  None:
                    return result[0], json.loads(result[1])
                else:
                    return defaultVal
            except sqlite3.OperationalError as e:
                print('ERROR AcceptorKorni3 _read sqlite3.OperationalError', e)
                return defaultVal
                #TODO
                #     sqlite3.OperationalError: no  such  table: PaxosAcceptor - 6AB
        finally:
            con.close()

    def _validate(self, register):
        # validate
        return partitionFromRegKey(register)  in self.partitions # != self.partitionId

    def prepare(self, register: str, ballot_number):
        if not self._validate(register): return None

        promise, accepted = self._read(register)
        #logger.info("prepare. name={0}. ballot_number={1}. promise={2}. accepted={3}".format(
        #    self.name, ballot_number, self.promise, self.accepted))

        if promise > ballot_number:
            return ("CONFLICT", "CONFLICT")
        else:
            #self.promise = ballot_number
            self._write(register, ballot_number, accepted)
            return accepted

    def accept(self, register: str, ballot_number, new_state):
        """
        8. Returns a conflict if it already saw a greater ballot number.
        9. Erases the promise, marks the received tuple (ballot number, value) as the accepted value and returns a confirmation
        """
        # logger.info("accept. name={0}. ballot_number={1}. new_state={2}. promise={3}. accepted={4}".format(
        #     self.name, ballot_number, new_state, self.promise, self.accepted))
        if not self._validate(register): return None
        promise, accepted = self._read(register)
        if promise > ballot_number:
            return ("CONFLICT", "CONFLICT")
        elif accepted[1] > ballot_number:
            # https://github.com/peterbourgon/caspaxos/blob/4374c3a816d7abd6a975e0e644782f0d03a2d05d/protocol/memory_acceptor.go#L118-L128
            return ("CONFLICT", "CONFLICT")
        # these two ought to be flushed to disk
        # http://rystsov.info/2015/09/16/how-paxos-works.html
        self._write(register, 0, (new_state, ballot_number) ) #self.promise = 0 ; self.accepted = (new_state, ballot_number)
        return ("CONFIRM", "CONFIRM")

    # handler are periodicaly called for check new req data
    def checkPrepareRequestsHandler(self):
        try:
            cur, con = self.cur()
            # TODO perf. add cond this and prev day by zT ?
            for partition in self.partitions:
                try:
                    cur.execute('select b,id, zT, zU from "' + self.preqtable(partition) +'" where aid=? and not handled',
                                     (self.mePubKey,))
                    for record in cur.fetchall():
                        ballot = record[0]
                        register = record[1]
                        zT = record[2]
                        proposerId = record[3]

                        mayBeConfirm = self.prepare(register, ballot)

                        def _sendPrepareResponse():
                            data = {"id": register, "b": ballot, "resp": mayBeConfirm,
                                    "handled": False, #"aid": self.zU   no need
                                    "proposerId": proposerId, "part": partition}
                            # repeat
                            def working(i):
                                p1 = subprocess.run(['korni3', 'insert', self.korni3container, self.presptable(partition)], input=json.dumps(data).encode('utf-8'),
                                                    stdout=PIPE, stderr=PIPE)
                                if p1.returncode == 1:
                                    print('ERROR Acceptor checkPrepareRequestsHandler ', p1.stdout.decode('utf-8'), p1.stderr.decode('utf-8'))
                                if p1.returncode == 100:
                                    print('ERROR db locked', p1.stderr.decode('utf-8'))
                                return p1.returncode
                            ret = 100
                            for i in range(0, 10):
                                if ret == 100:
                                    ret = working(i)
                                if ret == 100:
                                    sleep(3)
                                    print('need repeat - db locked ....')
                                    ret = working(i)
                            return ret == 0

                        if mayBeConfirm is not None:
                            isOk = _sendPrepareResponse()
                            if isOk:
                                # mark it handled for not handle again # its small hack but is normaly
                                cur.execute('update "'+self.preqtable(partition)+'" set handled = true where id=? and zT = ? and aid=?'
                                            , (register, zT, self.mePubKey))
                                cur.connection.commit()
                except sqlite3.OperationalError as e:
                    pass
                    # print('ERROR ', e)
        finally:
            con.close()

    # periodycaly called for check Accept REQ for me
    def checkAcceptRequestsHandler(self):
        try:
            cur, con = self.cur()
            for partition in self.partitions:
                try:
                    cur.execute('select reg, b, v, id, zT, zU '+
                                     'from "' + self.areqtable(partition) + '" where aid=?'+
                                     'and not handled\n'+
                                     '-- TODO zU is valid proposer\n'+
                                     '--order by zT DESC', (self.mePubKey,))
                    reqsformark = list()
                    for record in cur.fetchall():
                        register = record[0]
                        b = record[1]
                        v = record[2]
                        id = record[3]
                        zT = record[4]
                        zU = record[5]
                        aa,ab = self.accept(register, b, v)
                        # publish response
                        def _sendAcceptResponse():
                            # TODO handle retcode
                            data={"id": id, # same as in request
                                  "reg": register, "breq": b, "proposerId": zU,
                                  "acceptation": [aa,ab], "handled": False}

                            def working(i):
                                p1 = subprocess.run(['korni3', 'insert', self.korni3container, self.aresptable(partition)],
                                                   input=json.dumps(data).encode('utf-8'),
                                                   stdout=PIPE, stderr=PIPE)
                                if p1.returncode == 1:
                                    print('ERROR korni3 acceptor send accept resp', p1.stdout.decode('utf-8'), p1.stderr.decode('utf-8'))
                                return p1.returncode
                            ret = 100
                            for i in range(0, 10):
                                if ret == 100:
                                    ret = working(i)
                                if ret == 100:
                                    sleep(3)
                                    print('need repeat - db locked ....')
                                    ret = working(i)


                        _sendAcceptResponse()
                        # mark as handled
                        reqsformark.append([id,zT])

                    # mark req as handled
                    for r in reqsformark:
                        cur.execute('update "' + self.areqtable(partition) + '" set handled=1 where id=?'+
                                         ' and zT=?',
                                         (r[0], r[1]))
                    cur.connection.commit()
                except sqlite3.OperationalError:
                    pass
        finally:
            con.close()

class ProposerKorni3(object):

    """
    2. The proposer generates a ballot number, B, and sends "prepare" messages containing that number
    to the acceptors.

    5. Waits for the F + 1 confirmations
    6. If they all contain the empty value, then the proposer defines the current state as PHI otherwise it picks the value of the tuple with the highest ballot number.
    7. Applies the f function to the current state and sends the result, new state, along with the generated ballot number B (an "accept" message) to the acceptors.

    10. Waits for the F + 1 confirmations.
    11. Returns the new state to the client.
    """
    def __init__(self, acceptorsFunc, korni3container, partitions, mePubKey: str):
        self.acceptorsFunc = acceptorsFunc
        self.partitions = partitions
        self.mePubKey = mePubKey
        self.tableName = 'StateProposerLocal' # local state of proposer for registry

        # logger.info(
        #     "Init Proposer. acceptors={0}. F={1}. initial_state={2}".format(
        #         self.acceptors, self.F, self.state))

        self.korni3container = korni3container
        p1 = subprocess.run(["korni3", "db", self.korni3container], stdout=PIPE, stderr=PIPE)
        if p1.returncode != 0:
            print('ERROR korni3 get path to container', p1.stdout.decode('utf-8'), p1.stderr.decode('utf-8'))
            os.exit(1)
        self.path = p1.stdout.decode('utf-8').strip()

        try:
            cur, con = self.cur()
            # shure indexes
            def createIndex(name, table, createquery):
                try:
                    cur.execute('SELECT * FROM sqlite_master WHERE type="index" and tbl_name=? and name=? ',
                                     (table, name))
                    if not cur.fetchone():
                        cur.execute(createquery)
                except sqlite3.OperationalError :
                    pass
            for partition in self.partitions:
                createIndex("proposer-register", "StateProposerLocal", 'CREATE INDEX "proposer-register" ON "StateProposerLocal" ( 	"register" );')
                createIndex("presp-proposerid-handled-" + partition, "RESP-PREPARE-Part-" + partition,
                            'CREATE INDEX "presp-proposerid-handled-' + partition + '" ON "RESP-PREPARE-Part-' + partition + '" ( 	"proposerId", 	"handled" );')
                createIndex("presp-handled-proposer-b-id-" + partition, "RESP-PREPARE-Part-" + partition,
                            'CREATE INDEX "presp-handled-proposer-b-id-' + partition + '" ON "RESP-PREPARE-Part-' + partition + '" ( 	"handled", 	"proposerId", 	"b", 	"id" );')
                createIndex("aresp-proposerid-handled-" + partition, "RESP-ACCEPT-Part-" + partition,
                            'CREATE INDEX "aresp-proposerid-handled-' + partition + '" ON "RESP-ACCEPT-Part-' + partition + '" ( 	"proposerId", 	"handled" );')
                createIndex("aresp-handled-id-" + partition, "RESP-ACCEPT-Part-" + partition,
                            'CREATE INDEX "aresp-handled-id-' + partition + '" ON "RESP-ACCEPT-Part-' + partition + '" ( 	"handled", 	"id" );')
                createIndex("aresp-id-zT-" + partition, "RESP-ACCEPT-Part-" + partition,
                            'CREATE INDEX "aresp-id-zT-' + partition + '" ON "RESP-ACCEPT-Part-' + partition + '" ( 	"id", 	"zT" );')
                createIndex("inreq-handled-" + partition, "PAXOS-CHANGE-REQ-Part-" + partition,
                            'CREATE INDEX "inreq-handled-' + partition + '" ON "PAXOS-CHANGE-REQ-Part-' + partition + '" ( 	"handled" );')
                createIndex("inreq-id-zT-" + partition, "PAXOS-CHANGE-REQ-Part-" + partition,
                            'CREATE INDEX "inreq-id-zT-' + partition + '" ON "PAXOS-CHANGE-REQ-Part-' + partition + '" ( "id", 	"zT" );')
        finally:
            con.close()

    def cur(self):  # can be None
        con = sqlite3.connect(self.path, timeout=5)
        cur = con.cursor()
        return cur, con

    def inreqtable(self, partitionId):
        return 'PAXOS-CHANGE-REQ-Part-' + partitionId
    def outtable(self, partitionId):
        return 'PAXOS-RESULT-Part-' + partitionId
    def preqtable(self, partitionId):
        return 'REQ-PREPARE-Part-' + partitionId
    def presptable(self, partitionId):
        return 'RESP-PREPARE-Part-' + partitionId
    def areqtable(self, partitionId):
        return 'REQ-ACCEPT-Part-' + partitionId
    def aresptable(self, partitionId):
        return 'RESP-ACCEPT-Part-' + partitionId
    def acceptors(self, partition):
        return self.acceptorsFunc(partition)
    def F(self, partition):
        # since we need to have 2F+1 acceptors to tolerate F failures, then:
        return (len(self.acceptorsFunc(partition)) - 1) / 2
    def generate_ballot_number(self, notLessThan=0):
        """
        http://rystsov.info/2015/09/16/how-paxos-works.html
        Each server may have unique ID and use an increasing sequence of natural number n to generate (n,ID) tuples and use tuples as ballot numbers.
        To compare them we start by comparing the first element from each tuple. If they are equal, we use the second component of the tuple (ID) as a tie breaker.
        Let IDs of two servers are 0 and 1 then two sequences they generate are (0,0),(1,0),(2,0),(3,0).. and (0,1),(1,1),(2,1),(3,1).. Obviously they are unique, ordered and for any element in one we always can peak an greater element from another.
        """
        # we should never generate a random number that is equal to zero
        # since Acceptor.promise defaults to 0
        #ballot_number = random.randint(notLessThan + 1, 100)
        ballot_number = int(time.time())
        return ballot_number
    # clean function, no side effects
    def get_highest_confirmation(self, confirmations):
        ballots = []
        for i in confirmations:
            ballots.append(i[1])
        ballots = sorted(ballots)
        highestBallot = ballots[len(ballots) - 1]
        for i in confirmations:
            if i[1] == highestBallot:
                return i


    # start handling input request
    def receive(self, register: str, fId: str, nextValue, reqid):
        """
        receives f change function from client and begins consensus process.
        """
        #  Generate ballot number, B and sends 'prepare' msg with that number to the acceptors.
        ballot_number = self.generate_ballot_number()
        # logger.info("receive. change_func={0}. ballot_number={1}.".format(f, ballot_number))
        self.send_prepare(register, ballot_number=ballot_number, funcId=fId, nextValue=nextValue, reqid=reqid)
        # now wait confirmation quorum
        # and then prepare 2


    def _write(self, register: str, ballot, functionId: str, nextValue, state, reqid):
        data = {"id": json.dumps([register,ballot]),
                "register": register, "F": self.F(partitionFromRegKey(register)), "b": ballot,
                "fId": functionId, "nextValue": nextValue, "state": state,
                "reqid": reqid}

        def working(i):
            # , '--ignore'
            p1 = subprocess.run(['korni3', 'insert', self.korni3container, self.tableName],
                                input=json.dumps(data).encode('utf-8'),
                                stdout=PIPE, stderr=PIPE)
            if p1.returncode != 0:
                print('ERROR korni3 proposer write', p1.stdout.decode('utf-8'), p1.stderr.decode('utf-8'))
                # raise Exception(p1.stdout.decode('utf-8'), p1.stderr.decode('utf-8'))
            return p1.returncode
        ret = 100
        for i in range(0, 10):
            if ret == 100:
                ret = working(i)
            if ret == 100:
                sleep(3)
                print('need repeat - db locked ....')
                ret = working(i)

    def _read(self, register: str):
        """ return (b,fId,next,state, F, reqid) """
        try:
            cur, con = self.cur()
            cur.execute('select id, F, b, fId, state, nextValue, reqid '
                             'from "'+self.tableName+'" where register=? order by zT DESC limit 1 ',
                             (register,) )
            r = cur.fetchone()
            if not r is None:
                F = r[1]
                b = r[2]
                fId = r[3]
                state = r[4]
                next = r[5] #scalar
                try:
                    next = json.loads(next)
                except Exception:
                    next = None
                reqid = r[6]
                return (b,fId,next,state, F, reqid)
            else:
                return (0,'','',0, self.F(partitionFromRegKey(register)), 'NULL')
                # raise Exception('can not read proposer state')
        finally:
            con.close()

    def _sendPrepareRequest(self, acceptorPub: str, register: str, ballot):
        data = {"id": register, "type": 'prepare', "aid": acceptorPub, "b": ballot,
                "handled": False }

        def working(i):
            p1 = subprocess.run(['korni3', 'insert', self.korni3container, self.preqtable(partitionFromRegKey(register))],
                                input=json.dumps(data).encode('utf-8'),
                                stdout=PIPE, stderr=PIPE)
            if p1.returncode != 0:
                print('ERROR korni3 prep req', p1.stdout.decode('utf-8'), p1.stderr.decode('utf-8'))
            return p1.returncode
        ret = 100
        for i in range(0, 10):
            if ret == 100:
                ret = working(i)
            if ret == 100:
                sleep(3)
                print('need repeat - db locked ....')
                ret = working(i)
        return ret


    def send_prepare(self, register: str, ballot_number, funcId: str, nextValue, reqid):
        for acceptor in self.acceptors(partitionFromRegKey(register)):
            self._sendPrepareRequest(str(acceptor), register, ballot_number) # RPC. make request and wait response
            time.sleep(2) # TODO fix it
        # save state and wait resp ...
        self._write(register, ballot_number, funcId, nextValue, 0, reqid)


    def prepare2(self, register, ballot, funcId: str, confirmations): # set state for register
        total_list_of_confirmation_values = []
        for i in confirmations:
            total_list_of_confirmation_values.append(i[0])
        # If they(confirmations) all contain the empty value,
        # then the proposer defines the current state as PHI otherwise it picks the
        # value of the tuple with the highest ballot number.
        s_b, s_fId, s_next, _s_state, s_F, s_reqid = self._read(register)
        state = None
        if sum(total_list_of_confirmation_values) == 0:
            state = 00          # default value 00
        else:
            highest_confirmation = self.get_highest_confirmation(confirmations)
            state = highest_confirmation[0]
        self._write(register, s_b, s_fId, s_next, state, s_reqid)
        self.send_accept(register, funcId, ballot, state, s_next, s_reqid)


    def _sendAcceptRequest(self, acceptor, register, ballot_number, new_state):
        data={"id": json.dumps([register, ballot_number]),  "type": 'accept', "aid": acceptor,
              "reg":register,
              "b": ballot_number, "v": new_state, "handled": 0}

        def working(i):
            p1 = subprocess.run(['korni3', 'insert', self.korni3container, self.areqtable(partitionFromRegKey(register))],
                               input=json.dumps(data).encode('utf-8'))
            if p1.returncode != 0:
                print('ERROR korni3 proposer send accept req', p1.stdout.decode('utf-8'), p1.stderr.decode('utf-8'))
            return p1.returncode
        ret = 100
        for i in range(0, 10):
            if ret == 100:
                ret = working(i)
            if ret == 100:
                sleep(3)
                print('need repeat - db locked ....')
                ret = working(i)
        return ret


    def send_accept(self, register, fId, ballot_number, state, nextValue, reqid):
        """
        7. Applies the f function to the current state and sends the result, new state, along with the generated ballot number B (an "accept" message) to the acceptors.
        """
        # transform state and save it
        f = getFunction(fId)
        if not f:
            print('ERROR functio fId change function are not found fid=', fId)
        else:
            state = f(state, nextValue)
            self._write(register, ballot_number, fId, nextValue, state, reqid)
            for acceptor in self.acceptors(partitionFromRegKey(register)):
                self._sendAcceptRequest(acceptor, register, ballot_number, state) # RPC
                # time.sleep(1)  # TODO fix it
            # then wait and then when response quorum , and then - accept2


    def accept2(self, reg, b, breq, F, fId, state, reqid, isConfirm: bool):
        if str(b) == str(breq):
            data = {"id": reqid, "reg": reg, "fId": fId,
                    "b": b, "F": F,  # , "proposerId": self.mePubKey, - zU
                    "result": 'CONFIRM' if isConfirm else 'CONFLICT',
                    "state": state,
                    "handled": False}
            def working(i):
                # todo ? добавить в ответ все связанные документы с подписями ? или написать отдельный отчет?
                # todo проверить перед созданием ответа, что он еще не существует. ? --ignore
                p1 = subprocess.run(['korni3', 'insert', self.korni3container, self.outtable(partitionFromRegKey(reg)),
                                '--ignore'], input=json.dumps(data).encode('utf-8'), stdout=PIPE, stderr=PIPE)
                if p1.returncode != 0:
                    print('ERROR korni3 proposer accept2 ', p1.stdout.decode('utf-8'), p1.stderr.decode('utf-8'))
                return p1.returncode
            ret = 100
            for i in range(0, 10):
                if ret == 100:
                    ret = working(i)
                if ret == 100:
                    sleep(3)
                    print('need repeat - db locked ....')
                    ret = working(i)

    def checkInputRequest(self):
        try:
            cur, con = self.cur()
            for partition in self.partitions:
                try:
                    # TODO брать только то, что запросы мне непосредственно, а то все пропозеры партиции схватятзя за запрос
                    cur.execute('select id, zT, reg, fId, next ' 
                                     'from "' + self.inreqtable(partition) + '"  where not handled ')
                    fordelete = list()
                    for record in cur.fetchall():
                        id = record[0]
                        zT = record[1]
                        reg = record[2]
                        fId = record[3]
                        next = record[4]

                        s_b,s_fId,s_next,s_state, s_F, s_reqid = self._read(reg)
                        # TODO так то разумно, что продолжаем работать по старому запросу, но если ошибка - то это блокирует работу...
                        # if s_reqid != 'NULL' :
                        #     continue  # skip request . because now we already working

                        # start challenge
                        self.receive(reg, fId, next, id)

                        fordelete.append( (id, zT) )
                    # mark req as handled
                    for id, zT in fordelete:
                        cur.execute('update "' + self.inreqtable(partition) + '" set handled=1 where id=? and zT=?',
                                         (id, zT))
                        cur.connection.commit()
                except sqlite3.OperationalError:
                    pass
        finally:
            con.close()


    def checkPrepareConfirmsHandler(self):
        try:
            cur, con = self.cur()
            for partition in self.partitions:
                def confirms2():
                    try:
                        cur.execute('select id,zT,b,resp,zU from "' + self.presptable(partition) + '" '
                                         'where not handled and proposerId=?',
                                         (self.mePubKey, ))
                        confirmations = dict()
                        for record in cur.fetchall():
                            id = record[0]
                            b= record[2]
                            key = (id,b)
                            if not key in confirmations:
                                confirmations[key] = [record]
                            else:
                                confirmations[key].append(record)
                        for reg, b in confirmations:
                            records = confirmations[(reg,b)]
                            try:
                                s_b, fId, next, state, F, reqid = self._read(reg)
                                confirmsList = []
                                aids = set()
                                for r in records:
                                    resp = json.loads(r[3]) if r[3]!='NULL' else 0
                                    aid = r[4]  # todo check AID
                                    if aid not in aids:
                                        aids.add(aid)
                                        confirmsList.append(resp)
                                if len(confirmsList) >= F + 1:
                                    # we has достаточно confirms
                                    self.prepare2(reg, b, fId, confirmsList)

                                    # mark req as handled
                                    cur.execute('update "' + self.presptable(partition) +
                                                     '"  set handled=1 where proposerId=? ' + 'and b=? and id=?  ',
                                                 (self.mePubKey, b, reg))
                                    cur.connection.commit()
                            except Exception as e:
                                print('ERROR proposer not know what hapens here. i get response for what? i have not request for it!', e)
                    except sqlite3.OperationalError:
                        pass
                confirms2()
        finally:
            con.close()

    def checkAcceptConfirmsHandler(self):
        try:
            cur, con = self.cur()
            for partition in self.partitions:
                try:
                    cur.execute('select id, zT, breq, reg, acceptation from "' + self.aresptable(partition) + '" ' +
                                     'where "proposerId"=? 	AND not handled', (self.mePubKey, ))
                    confirmations = dict()
                    for record in cur.fetchall():
                        id = record[0]
                        reg, b = json.loads(id)
                        zT = record[1]
                        breq = record[2]
                        reg = record[3]
                        acceptation = json.loads(record[4])
                        #
                        info = {"id": id, "zT": zT, "b": b, "breq": breq, "acceptation": acceptation}
                        if (reg,breq) not in confirmations:
                            confirmations[(reg,breq)] = [info]
                        else:
                            confirmations[(reg,breq)].append(info)
                    for reg, breq in confirmations:
                        infoList = confirmations[(reg, breq)]
                        b, fId, next, state, F , reqid = self._read(reg)
                        # F - на момент запроса сохранена в базе
                        # но на момент ответа Proposer может быть пересоздан с другим списком acceptors
                        #  считаем что текущий список acceptors согласован и можно бы пересчитать данный F
                        # ПОДУМАТЬ ОБ ЭТОМ!
                        if len(infoList) >= F + 1:
                            # число ответов достаточно чтобы вычислить результат и сообщить о нем
                            def _publishResult(isConfirm: bool):
                                self.accept2(reg, b, breq, F, fId, state, reqid, isConfirm)
                            def _makrConfirmsAsHandled():
                                cur.execute('update "' + self.aresptable(partition) + '" set handled=1 where id=?',
                                                 (json.dumps([reg, breq]), ))
                                cur.connection.commit()
                                self._write(reg,b,None,None, None, None)
                                # now proposer can work with reg again in other req
                            # confirm OR conflict ?
                            countConfirm = 0
                            countConflict = 0
                            for i in infoList:
                                if i["acceptation"][0] != "CONFLICT":
                                    countConfirm = countConfirm + 1
                                else:
                                    countConflict = countConflict + 1

                            if countConfirm >= F + 1:
                                _publishResult(True)
                                _makrConfirmsAsHandled()

                            if countConflict >= F + 1:
                                _publishResult(False)
                                # если ответ is CONFLICT тоже пометить для удаления
                                def _conflictRespAsHandled():
                                    for i in infoList:
                                        cur.execute('update "' + self.aresptable(partition) + '" set handled=1 where '
                                                         + ' id=? and zT=?', (i["id"], i["zT"]))
                                    cur.connection.commit()
                                _conflictRespAsHandled()
                except sqlite3.OperationalError as e:
                    if 'no such table:' not in str(e):
                        print('ERROR when handle results', e)
        finally:
            con.close()

