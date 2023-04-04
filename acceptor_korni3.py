import hashlib
import json
import sqlite3
import subprocess
import uuid
import pause, datetime, time

# TODO запрос - это новая особая запись, на ее реагируем, обарабтываем.
# у нее id uuid
# в ответ создаем запись - ответ, у нее id тот же uuid как у запроса.

# export TODO
# all keys split to 4096 partitions -> 4096 each kind tables in container
def partitionFromRegKey(regKey: str) -> str:
    return hashlib.sha256(regKey.encode('utf-8')).hexdigest()[:3].upper()


class AcceptorKorni3(object) :
    #partitionId
    #tableName data table name
    #zU public key of user for control comunication

    # promise = 0  # ballot number
    # accepted = (0, 0)

    #korni3container : str container name
    #cur : SQlite3 cursor

    def __init__(self, partition: str, korni3container: str, zU: str):
        self.partitionId = partition
        self.tableName = 'PaxosAcceptor'+'-' + partition
        self.mePubKey = zU

        self.preqtable =  'REQ-PREPARE-Part-' + partition
        self.presptable = 'RESP-PREPARE-Part-' + partition
        self.areqtable =  'REQ-ACCEPT-Part-' + partition
        self.aresptable = 'RESP-ACCEPT-Part-' + partition


        self.korni3container = korni3container
        r = subprocess.run(["korni3", "db", self.korni3container], capture_output=True)
        path = r.stdout.decode('utf-8').strip()
        con = sqlite3.connect(path)
        self.cur = con.cursor()


        def createIndex(name, table, createquery):
            try:
                self.cur.execute('SELECT * FROM sqlite_master WHERE type="index" and tbl_name=? and name=? ',
                                 (table, name))
                if not self.cur.fetchone():
                    self.cur.execute(createquery)
            except sqlite3.OperationalError: pass
        createIndex("preq-aid-handled-"+self.partitionId, "REQ-PREPARE-Part-"+self.partitionId,
                    'CREATE INDEX "preq-aid-handled-'+self.partitionId+
                    '" ON "REQ-PREPARE-Part-'+self.partitionId+'" ( 	"aid", 	"handled" );')

        createIndex("preq-handled-id-zT-"+self.partitionId , "REQ-PREPARE-Part-"+self.partitionId,
                    'CREATE INDEX "preq-handled-id-zT-'+self.partitionId+'" ON "REQ-PREPARE-Part-'
                    +self.partitionId+'" ( 	"handled", 	"id", 	"zT" );')

        createIndex("areq-aid-handled-"+self.partitionId, "REQ-ACCEPT-Part-"+self.partitionId,
                    'CREATE INDEX "areq-aid-handled-'+self.partitionId+
                    '" ON "REQ-ACCEPT-Part-'+self.partitionId+'" (	"aid",	"handled");')

        createIndex("areq-handled-aid-zT-"+self.partitionId, "REQ-ACCEPT-Part-"+self.partitionId,
                    'CREATE INDEX "areq-handled-aid-zT-'+self.partitionId+
                    '" ON "REQ-ACCEPT-Part-'+self.partitionId+'" (	"handled", 	"aid", 	"zT" );')
        self.cur.connection.commit()


    def _write(self, register: str, promise, accepted): # return void
        if self._validate(register): return None

        data = {"id": register, "promise": promise, "accepted": accepted}
        # , '--ignore'
        p2 = subprocess.run(['korni3', 'insert', self.korni3container, self.tableName], input=json.dumps(data).encode('utf-8'), capture_output=True)

    def _read(self, registerKey: str) :  # return tuple ( promise, accepted: (0,0) )
        if self._validate(registerKey): return None
        defaultVal = (0, (0,0))
        try:
            self.cur.execute('select promise, accepted from "' + self.tableName +'"  where id=? and zU=?   ORDER by zT DESC limit 1',
                             (registerKey, self.mePubKey))
            result = self.cur.fetchone()
            if result is not  None:
                return result[0], json.loads(result[1])
            else:
                return defaultVal
        except sqlite3.OperationalError as e:
            print('ERROR AcceptorKorni3 _read sqlite3.OperationalError', e)
            return defaultVal
            #TODO
            #     sqlite3.OperationalError: no  such  table: PaxosAcceptor - 6AB

    def _validate(self, register):
        # validate
        return partitionFromRegKey(register) != self.partitionId

    def prepare(self, register: str, ballot_number):
        if self._validate(register): return None

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
        if self._validate(register): return None
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
        # TODO perf. add cond this and prev day by zT ?
        try:
            self.cur.execute('select b,id, zT, zU from "' + self.preqtable +'" where aid=? and not handled',
                             (self.mePubKey,))
            for record in self.cur.fetchall():
                ballot = record[0]
                register = record[1]
                zT = record[2]
                proposerId = record[3]

                mayBeConfirm = self.prepare(register, ballot)

                def _sendPrepareResponse():
                    data = {"id": register, "b": ballot, "resp": mayBeConfirm,
                            "handled": False, #"aid": self.zU   no need
                            "proposerId": proposerId, "part": self.partitionId}
                    p1 = subprocess.run(['korni3', 'insert', self.korni3container, self.presptable], input=json.dumps(data).encode('utf-8'), capture_output=True)
                    if p1.returncode > 0:
                        print('ERROR Acceptor checkPrepareRequestsHandler ', p1.stdout.read())
                _sendPrepareResponse()

                # mark it handled for not handle again # its small hack but is normaly
                self.cur.execute('update "'+self.preqtable+'" set handled = true where id=? and zT = ?', (register, zT))
                self.cur.connection.commit()
        except sqlite3.OperationalError as e:
            print('ERROR ', e)

    # periodycaly called for check Accept REQ for me
    def checkAcceptRequestsHandler(self):
        try:
            self.cur.execute('select reg, b, v, id, zT, zU '+
                             'from "' + self.areqtable + '" where aid=?'+
                             'and not handled\n'+
                             '-- TODO zU is valid proposer\n'+
                             '--order by zT DESC', (self.mePubKey,))
            reqsformark = list()
            for record in self.cur.fetchall():
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
                    p = subprocess.run(['korni3', 'insert', self.korni3container, self.aresptable], input=json.dumps(data).encode('utf-8'), capture_output=True)
                _sendAcceptResponse()
                # mark as handled
                reqsformark.append([id,zT])

            # mark req as handled
            for r in reqsformark:
                self.cur.execute('update "' + self.areqtable + '" set handled=1 where id=?'+
                                 ' and zT=?',
                                 (r[0], r[1]))
            self.cur.connection.commit()
        except sqlite3.OperationalError:
            pass

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
    def __init__(self, acceptors, korni3container, partitionId: str, mePubKey: str):
        self.mePubKey = mePubKey
        self.tableName = 'StateProposerLocal' # local state of proposer for registry
        self.partitionId = partitionId
        self.inreqtable = 'PAXOS-CHANGE-REQ-Part-' + partitionId
        self.outtable =   'PAXOS-RESULT-Part-' + partitionId
        self.preqtable =  'REQ-PREPARE-Part-' + partitionId
        self.presptable = 'RESP-PREPARE-Part-' + partitionId
        self.areqtable =  'REQ-ACCEPT-Part-' + partitionId
        self.aresptable = 'RESP-ACCEPT-Part-' + partitionId

        # in fact most times they usually are.
        # So we should add logic to handle that fact.
        if not isinstance(acceptors, list):
            raise ValueError("acceptors ought to be a list of child classes of Acceptor object")
        self.acceptors = acceptors
        # since we need to have 2F+1 acceptors to tolerate F failures, then:
        self.F = (len(self.acceptors) - 1) / 2

        # logger.info(
        #     "Init Proposer. acceptors={0}. F={1}. initial_state={2}".format(
        #         self.acceptors, self.F, self.state))

        self.korni3container = korni3container
        r = subprocess.run(["korni3", "db", self.korni3container], capture_output=True)
        path = r.stdout.decode('utf-8').strip()
        con = sqlite3.connect(path)
        self.cur = con.cursor()

        # shure indexes
        def createIndex(name, table, createquery):
            try:
                self.cur.execute('SELECT * FROM sqlite_master WHERE type="index" and tbl_name=? and name=? ',
                                 (table, name))
                if not self.cur.fetchone():
                    self.cur.execute(createquery)
            except sqlite3.OperationalError: pass
        createIndex("proposer-register", "StateProposerLocal", 'CREATE INDEX "proposer-register" ON "StateProposerLocal" ( 	"register" );')
        createIndex("presp-proposerid-handled-" + self.partitionId, "RESP-PREPARE-Part-" + self.partitionId,
                    'CREATE INDEX "presp-proposerid-handled-' + self.partitionId + '" ON "RESP-PREPARE-Part-' + self.partitionId + '" ( 	"proposerId", 	"handled" );')
        createIndex("presp-handled-proposer-b-id-" + self.partitionId, "RESP-PREPARE-Part-" + self.partitionId,
                    'CREATE INDEX "presp-handled-proposer-b-id-' + self.partitionId + '" ON "RESP-PREPARE-Part-' + self.partitionId + '" ( 	"handled", 	"proposerId", 	"b", 	"id" );')
        createIndex("aresp-proposerid-handled-" + self.partitionId, "RESP-ACCEPT-Part-" + self.partitionId,
                    'CREATE INDEX "aresp-proposerid-handled-' + self.partitionId + '" ON "RESP-ACCEPT-Part-' + self.partitionId + '" ( 	"proposerId", 	"handled" );')
        createIndex("aresp-handled-id-" + self.partitionId, "RESP-ACCEPT-Part-" + self.partitionId,
                    'CREATE INDEX "aresp-handled-id-' + self.partitionId + '" ON "RESP-ACCEPT-Part-' + self.partitionId + '" ( 	"handled", 	"id" );')
        createIndex("aresp-id-zT-" + self.partitionId, "RESP-ACCEPT-Part-" + self.partitionId,
                    'CREATE INDEX "aresp-id-zT-' + self.partitionId + '" ON "RESP-ACCEPT-Part-' + self.partitionId + '" ( 	"id", 	"zT" );')
        createIndex("inreq-handled-" + self.partitionId, "PAXOS-CHANGE-REQ-Part-" + self.partitionId,
                    'CREATE INDEX "inreq-handled-' + self.partitionId + '" ON "PAXOS-CHANGE-REQ-Part-' + self.partitionId + '" ( 	"handled" );')
        createIndex("inreq-id-zT-" + self.partitionId, "PAXOS-CHANGE-REQ-Part-" + self.partitionId,
                    'CREATE INDEX "inreq-id-zT-' + self.partitionId + '" ON "PAXOS-CHANGE-REQ-Part-' + self.partitionId + '" ( "id", 	"zT" );')

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

    def _write(self, register: str, ballot, functionId: str, nextValue, state, reqid):
        data = {"id": json.dumps([register,ballot]),
                "register": register, "F": self.F, "b": ballot,
                "fId": functionId, "nextValue": nextValue, "state": state,
                "reqid": reqid}
        # , '--ignore'
        p2 = subprocess.run(['korni3', 'insert', self.korni3container, self.tableName], input=json.dumps(data).encode('utf-8'), capture_output=True)

    def _read(self, register: str):
        """ return (b,fId,next,state, F, reqid) """
        self.cur.execute('select id, F, b, fId, state, nextValue, reqid '
                         'from "'+self.tableName+'" where register=? order by zT DESC limit 1 ',
                         (register,) )
        r = self.cur.fetchone()
        if not r is None:
            F = r[1]
            b = r[2]
            fId = r[3]
            state = r[4]
            next = r[5]
            reqid = r[6]
            return (b,fId,next,state, F, reqid)
        else:
            raise Exception('can not read proposer state')

    def _sendPrepareRequest(self, acceptorPub: str, register: str, ballot):
        data = {"id": register, "type": 'prepare', "aid": acceptorPub, "b": ballot,
                "handled": False }
        p1 = subprocess.run(['korni3', 'insert', self.korni3container, self.preqtable], input=json.dumps(data).encode('utf-8'), capture_output=True)
        return p1.returncode

    def send_prepare(self, register: str, ballot_number, funcId: str, nextValue, reqid):
        for acceptor in self.acceptors:
            self._sendPrepareRequest(str(acceptor), register, ballot_number) # RPC. make request and wait response
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

    def _sendAcceptRequest(self, acceptor, register, ballot_number, new_state):
        data={"id": json.dumps([register, ballot_number]),  "type": 'accept', "aid": acceptor,
              "reg":register,
              "b": ballot_number, "v": new_state, "handled": 0}
        p = subprocess.run(['korni3', 'insert', self.korni3container, self.areqtable], input=json.dumps(data).encode('utf-8'))

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
            for acceptor in self.acceptors:
                self._sendAcceptRequest(acceptor, register, ballot_number, state) # RPC
            # then wait and then when response quorum , and then - accept2

    def accept2(self, reg, b, breq, F, fId, state, reqid, isConfirm: bool):
        if str(b) == str(breq):
            data = {"id": reqid, "reg": reg, "fId": fId,
                    "b": b, "F": F,  # , "proposerId": self.mePubKey, - zU
                    "result": 'CONFIRM' if isConfirm else 'CONFLICT',
                    "state": state,
                    "handled": False}
            # todo ? добавить в ответ все связанные документы с подписями ? или написать отдельный отчет?
            # todo проверить перед созданием ответа, что он еще не существует. ? --ignore
            subprocess.run(['korni3', 'insert', self.korni3container, self.outtable, '--ignore'], input=json.dumps(data).encode('utf-8'), capture_output=True)

    def checkPrepareConfirmsHandler_RESP_PREPARE(self):
        def confirms2():
            try:
                self.cur.execute('select id,zT,b,resp,zU from "' + self.presptable + '" '
                                 'where not handled and proposerId=?',
                                 (self.mePubKey, ))
                confirmations = dict()
                for record in self.cur.fetchall():
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
                            resp = json.loads(r[3])
                            aid = r[4]  # todo check AID
                            if aid not in aids:
                                aids.add(aid)
                                confirmsList.append(resp)
                        if len(confirmsList) >= F + 1:
                            # we has достаточно confirms
                            self.prepare2(reg, b, fId, confirmsList)
                            # mark req as handled
                            self.cur.execute('update "' + self.presptable + '"  set handled=1 where proposerId=? ' +
                                         'and b=? and id=?  ',
                                         (self.mePubKey, b, reg))
                        self.cur.connection.commit()
                    except Exception as e:
                        print('ERROR proposer not know what hapens here. i get response for what? i have not request for it!', e)
            except sqlite3.OperationalError:
                pass
        confirms2()

    def checkAcceptConfirmsHandler(self):
        try:
            self.cur.execute('select id, zT, breq, reg, acceptation from "' + self.aresptable + '" ' +
                             'where "proposerId"=? 	AND not handled',
                             (self.mePubKey, ))
            confirmations = dict()
            for record in self.cur.fetchall():
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
                        self.cur.execute('update "' + self.aresptable + '" set handled=1 where id=?',
                                         (json.dumps([reg, breq]), ))
                        self.cur.connection.commit()
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
                                self.cur.execute('update "' + self.aresptable + '" set handled=1 where ' +
                                                 ' id=? and zT=?',
                                                 (i["id"], i["zT"]))
                            self.cur.connection.commit()
                        _conflictRespAsHandled()
        except sqlite3.OperationalError:
            pass

    def checkInputRequest(self):
        self.cur.execute('select id, zT, reg, fId, next ' 
                         'from "' + self.inreqtable + '"  where not handled ')
        fordelete = list()
        for record in self.cur.fetchall():
            id = record[0]
            zT = record[1]
            reg = record[2]
            fId = record[3]
            next = record[4]

            self.receive(reg,fId,next, id)

            fordelete.append( (id, zT) )
        #mark req as handled
        for id, zT in fordelete:
            self.cur.execute('update "' + self.inreqtable + '" set handled=1 where id=? and zT=?',
                             (id, zT))
            self.cur.connection.commit()


#################################### CLIENT API

# make REQUEST FOR CHANGE
def paxosKorni3ClientRequest(key: str, function_id: str, nextValue, korni3container='paxos'):
    table = 'PAXOS-CHANGE-REQ-Part-'+partitionFromRegKey(key)
    id = str(uuid.uuid4())
    data = {"id": id, "reg": key, "fId": function_id, "next": nextValue, "handled": False}
    p = subprocess.run(['korni3', 'insert', korni3container, table], input=json.dumps(data).encode('utf-8'))
    if p.returncode != 0:
        print('ERROR run korni3 ', p.stderr.read(), p.stdout.read())
    else:
        return id

# return function for check values
def paxosKorni3GetChecker(containerName: str):
    r = subprocess.run(["korni3", "db", containerName], capture_output=True)
    path = r.stdout.decode('utf-8').strip()
    con = sqlite3.connect(path)
    cur = con.cursor()
    def createIndex(name, table, createquery):
        try:
            cur.execute('SELECT * FROM sqlite_master WHERE type="index" and tbl_name=? and name=? ',
                             (table, name))
            if not self.cur.fetchone():
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
                # mark as handled
                cur.execute('update "PAXOS-RESULT-Part-'+partitionId+'" set handled=1 where id=? and zT=?', (r[1], r[2]))
                con.commit()
                return val if val!='CONFLICT' else None # TODO ? сделать чтобы значение могло быть CONFLICT. надо просто больше тестировать и доулучшить
            except TypeError:
                return None
        except sqlite3.OperationalError:
            return None
    return paxosKorni3ClientCheckResult

if __name__ == '__main__':
    # sure, is container are exists ?
    from pathlib import Path
    subprocess.run(['mkdir', '-p', str(Path.home())+'/.config/korni/paxos/'])
    mePub = 'MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAvJQG4135BoAhgw+/X+11XOqj2d3BVSX61pLb7EKiPUirzoNo15nP/5ZFrGB00vTfiuGbkP/puBDXMiNnNQNKVHxJ8Ltu/W1Fxo9k5UiFvQ5mmml+ZolkqzKgHaDTM9a9OQ96mc4oelSsX81Nh5+5hsP8RmUHZ8A/pImqr3Ttjmx4BBz+m9fccgvMi2cjG/a79k5/Lv9AYO28MHImHt0whzzHyrKWGeLxEdyPVRrfgbp/crw0lQJsvWZUTkiKFSK8ur0qFvg7szhu+8AOIDcTV9me/uZgOAeEiohn8/KN8rkAlgdFVBmWyyO/KOHR2OK7+PeQDOIxn33xQ0KE9g5aN6lHgRljewsWieP1PNAH6KZCszSykIvdYD5fvniGrOxxJN4HI0CY9q/QWKHYxUzckXtuYuLGiXT5/FAyPdefUGVBt8oB2Nq/kWH4qgu325fdpj0HWYwSPIJWAwQL0/c/XzlJwEjScR+pw/CoMtw1IWDWd5Xx/a8N9hz6FtUiPVNyq8fVjiLwc8cRgo2SrVA2EV4yPRzNcBDGb+r1h5k1dNhc1ATGEJQUWG7zOa4Jj7m0xWoDBIuGFctt6gaIUYZyTHwecIQIjj0nvefW/4reXMGvjBhM+91qqq4zRVI9qWdftJXuE161YZbEWN5xfm3dfCa8udq5m3zEu7HktSlKBD0CAwEAAQ=='

    partitionK1 = partitionFromRegKey('k1')
    print('partitionK1', partitionK1)
    partitionK2 = partitionFromRegKey('k2')

    a1 = AcceptorKorni3(partition=partitionK1, korni3container='paxos', zU=mePub)
    print('a1.prepare 12', a1.prepare('k1', 12))
    print('a1.accept(k1,11, 3)', a1.accept('k1',11, 3))
    print("a1._read('k1')", a1._read('k1'))
    print('a1.prepare 101', a1.prepare('k1', 101))
    print("a1._read('k1')", a1._read('k1'))
    print('a1.accept(k1,101, 3)', a1.accept('k1',101, 3))
    a1.checkPrepareRequestsHandler()

    # TODO version CAS
    def change_func(state, nextValue):
        """
        http://rystsov.info/2015/09/16/how-paxos-works.html
        It's a common practice for storages to have write operations to mutate its state and read operations to query it.
        Paxos is different, it guarantees consistency only for write operations,
        so to query/read its state the system makes read, writes the state back and when the state change is accepted the system queries the written state.

        ie:
        It's impossible to read a value in Single Decree Paxos without modifying the state of the system.
        For example if you connect only to one acceptor then you may get stale read.
        If you connect to a quorum of the acceptors then each acceptor may return a different value, so you should pick a value with the largest ballot number and send it back to acceptors.
        Once you get a confirmation of the successful write then you can return the written value to the client as the current value.
        So in order to read you should write.

        ie:
        https://twitter.com/rystsov/status/971796687642550277
        There is no native read or write operations in CASPaxos, the only primitive is change:
        apply a function to the stored value and return an updated value to a client.
        If the function is "lambda x: x" we get a read, if it's "lambda x: x+1" then it's increment
        and the "lambda x: f(x) if p(x) else x" is conditional write.
        So from this perspective there reads are almost indistinguishable from writes.
        """
        if state == 0:
            return {"version": 0, value: 0}

        if type(state)==dict and 'version' in state   and   (nextValue['version']==state['version'] + 1) :
            return nextValue
        else:
            return state

        return state + 3
    def read_func(state, nextValue):
        return state
    def set_func(state, nextValue):
        return state + 3

    def getFunction(id:str):
        if id=='change_func':
            return change_func
        if id=='read_func':
            return read_func
        if id=='set_func':
            return set_func

    acceptorsList = [mePub]
    # TODO create acceptors objects for range of partotions

    # todo make name a env param
    p = ProposerKorni3(acceptors=acceptorsList,
                       korni3container='paxos',
                       partitionId=partitionK1,
                       mePubKey=mePub)
    # req1id = paxosKorni3ClientRequest('k1', 'read_func', None)
    # print('req1id', req1id)
    #p.checkInputRequest()
    # p.receive('k1', 'change_func', 3)
    # p.receive('k1', 'read_func', None)
    req1id = paxosKorni3ClientRequest('k1', 'read_func', 'NULL')
    paxosKorni3ClientCheckResult = paxosKorni3GetChecker('paxos')

    ####################### handle requests and confirm and messages founded in files
    def _checkAcceptorsReq(acceptorsObects):
        for a in acceptorsObects:
            a.checkPrepareRequestsHandler()
            a.checkAcceptRequestsHandler()
    def _checkPoposerRequests(proposerObject):
        proposerObject.checkInputRequest()
        proposerObject.checkPrepareConfirmsHandler_RESP_PREPARE()
        proposerObject.checkAcceptConfirmsHandler()

    # a1 - acceptor.   [a1] - acceptors list
    # p - proposer

    CHECK_PERIOD = datetime.timedelta(seconds=3) # sec  # TODO make it a env var
    while True:
        t1 = datetime.datetime.now()
        t2 = t1 + CHECK_PERIOD
        if True:
            # stuff jobs here
            _checkAcceptorsReq([a1]) # TODO MAKE it a param
            _checkPoposerRequests(p)

            v = paxosKorni3ClientCheckResult(req1id, 'k1') # TODO REMOVE. is test
            if not v is None : print('v=', v)
            # jobs end
        pause.until(t2)
    #######################