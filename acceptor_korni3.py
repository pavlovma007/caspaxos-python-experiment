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
        self.tableName = 'PaxosAcceptor'+'-'+self.partitionId
        self.mePubKey = zU
        #r = subprocess.run(["korni3", "db", self.korni3container], capture_output=True)

        self.korni3container = korni3container
        r = subprocess.run(["korni3", "db", self.korni3container], capture_output=True)
        path = r.stdout.decode('utf-8').strip()
        con = sqlite3.connect(path)
        self.cur = con.cursor()

    def _write(self, register: str, promise, accepted): # return void
        if self._validate(register): return None

        # RUN korni3 insert new version for ...
        # korni3 insert  self.korni3container PaxosAcceptor<PartitionId>
        # {"id": register, "promise": , "accepted": }

        data = {"id": register, "promise": promise, "accepted": accepted}
        # data_str = json.dumps(data)
        p2 = subprocess.run(['korni3', 'insert', self.korni3container, self.tableName], # , '--ignore'
                            input=json.dumps(data).encode('utf-8'), capture_output=True)

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
        #self.promise = 0
        #self.accepted = (new_state, ballot_number)
        self._write(register, 0, (new_state, ballot_number) )

        return ("CONFIRM", "CONFIRM")

    # handler are periodicaly called for check new req data
    def checkPrepareRequestsHandler(self):
        preqtable = 'REQ-PREPARE-Part-' + self.partitionId
        presptable = 'RESP-PREPARE-Part-' + self.partitionId

        # TODO perf. add cond this and prev day by zT ?
        try:
            self.cur.execute('select b,id, zT, zU from "' + preqtable +'" where aid=? and not handled',
                             (self.mePubKey,))
            for record in self.cur.fetchall():
                ballot = record[0]
                register = record[1]
                zT = record[2]
                proposerId = record[3]

                mayBeConfirm = self.prepare(register, ballot)


                def _sendPrepareResponse():
                    data = {"id": register, "b": ballot,"resp": mayBeConfirm, "handled": False,
                            #"aid": self.zU   no need
                            "proposerId": proposerId, "part": self.partitionId}
                    p1 = subprocess.run(['korni3', 'insert', self.korni3container, presptable],
                                   input=json.dumps(data).encode('utf-8'), capture_output=True)
                    if p1.returncode > 0:
                        print('ERROR Acceptor checkPrepareRequestsHandler ', p1.stdout.read()) # TODO
                _sendPrepareResponse()

                # mark it handled for not handle again # its small hack but is normaly
                self.cur.execute('update "'+preqtable+'" set handled = true where id=? and zT = ?', (register, zT))
                self.cur.connection.commit()
        except sqlite3.OperationalError as e:
            print('ERROR ', e)

    # periodycaly called for check Accept REQ for me
    def checkAcceptRequestsHandler(self):
        areqtable = 'REQ-ACCEPT-Part-'+self.partitionId
        aresptable = 'RESP-ACCEPT-Part-'+self.partitionId
        self.cur.execute('select reg, b, v, id, zT, zU '+
                         'from "' + areqtable + '" where aid=?'+
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
                p = subprocess.run(['korni3', 'insert', self.korni3container, aresptable],
                                   input=json.dumps(data).encode('utf-8'), capture_output=True)
            _sendAcceptResponse()
            # mark as handled
            reqsformark.append([id,zT])

        # mark req as handled
        for r in reqsformark:
            self.cur.execute('update "'+areqtable+'" set handled=1 where id=?'+
                             ' and zT=?',
                             (r[0], r[1]))
        self.cur.connection.commit()


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

    #acceptors acceptor list . TODO WHAT IS IT?
    #state 0,1,2
    #F number from paper (like quorum)

    # mePubKey
    #korni3container
    #cur sqlite cursor
    #partitionId

    def __init__(self, acceptors, korni3container, partitionId: str, mePubKey: str):
        self.mePubKey = mePubKey
        self.tableName = 'StateProposerLocal' # local state of proposer for registry
        self.partitionId = partitionId

        # TODO: note that a node can be both a proposer and an acceptor at the same time
        # in fact most times they usually are.
        # So we should add logic to handle that fact.
        if not isinstance(acceptors, list):
            raise ValueError("acceptors ought to be a list of child classes of Acceptor object")
        self.acceptors = acceptors
        # since we need to have 2F+1 acceptors to tolerate F failures, then:
        self.F = (len(self.acceptors) - 1) / 2
        #self.state = 0

        # logger.info(
        #     "Init Proposer. acceptors={0}. F={1}. initial_state={2}".format(
        #         self.acceptors, self.F, self.state))

        self.korni3container = korni3container
        r = subprocess.run(["korni3", "db", self.korni3container], capture_output=True)
        path = r.stdout.decode('utf-8').strip()
        con = sqlite3.connect(path)
        self.cur = con.cursor()

        # todo shure index. register
        # REQ-PREPARE-Part-6AB    aid b handled
        # RESP-PREPARE-Part-6AB   b handled proposerId zU

    def receive(self, register: str, fId: str, nextValue):
        """
        receives f change function from client and begins consensus process.
        """
        #  Generate ballot number, B and sends 'prepare' msg with that number to the acceptors.
        ballot_number = self.generate_ballot_number()
        # logger.info("receive. change_func={0}. ballot_number={1}.".format(f, ballot_number))
        self.send_prepare(register, ballot_number=ballot_number, funcId=fId, nextValue=nextValue)
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

    def _write(self, register: str, ballot, functionId: str, nextValue, state=0):
        uid = str(uuid.uuid4()) # always new record
        data = {"id": json.dumps([register,ballot]),
                "register": register, "F": self.F, "b": ballot,
                "fId": functionId, "nextValue": nextValue, "state": state}
        p2 = subprocess.run(['korni3', 'insert', self.korni3container, self.tableName],  # , '--ignore'
                            input=json.dumps(data).encode('utf-8'), capture_output=True)

    def _read(self, register: str):
        """ return (b,fId,next,state, F) """
        self.cur.execute('select id, F, b, fId, state, nextValue from "'+self.tableName+'" where id=? order by zT DESC limit 1 ',
                         (register,) )
        r = self.cur.fetchone()

        F = r[1]
        b = r[2]
        fId = r[3]
        state = r[4]
        next = r[5]
        # return (F, b, fId, state, next)
        return (b,fId,next,state, F)
    def _readPConfirmations(self, registry, ballot):
        pass

    def _sendPrepareRequest(self, acceptorPub: str, register: str, ballot):
        data = {"id": register, "type": 'prepare', "aid": acceptorPub, "b": ballot,
                "handled": False }

        # print(hashlib.sha256(register.encode('utf-8')).hexdigest()[:3].upper())
        partition = partitionFromRegKey(register)

        tableName = 'REQ-PREPARE-Part-'+str(partition)
        p1 = subprocess.run(['korni3', 'insert', self.korni3container, tableName], input=json.dumps(data).encode('utf-8'), capture_output=True)
        return p1.returncode

    def send_prepare(self, register: str, ballot_number, funcId: str, nextValue):
        for acceptor in self.acceptors:
            # RPC. make request and wait response
            self._sendPrepareRequest(str(acceptor), register, ballot_number)
        # save state and wait resp ...
        self._write(register, ballot_number, funcId, nextValue)

    def prepare2(self, register, ballot, funcId: str, confirmations): # set state for register
        total_list_of_confirmation_values = []
        for i in confirmations:
            total_list_of_confirmation_values.append(i[0])

        # If they(confirmations) all contain the empty value,
        # then the proposer defines the current state as PHI otherwise it picks the
        # value of the tuple with the highest ballot number.
        s_b, s_fId, s_next, s_state, s_F = self._read(register)
        state = None
        if sum(total_list_of_confirmation_values) == 0:
            state = 00          # default value 00
        else:
            highest_confirmation = self.get_highest_confirmation(confirmations)
            state = highest_confirmation[0]
        self._write(register, s_b, s_fId, s_next, state)

        self.send_accept(register, funcId, ballot, state, s_next)

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
        table = 'REQ-ACCEPT-Part-' + self.partitionId
        data={"id": json.dumps([register, ballot_number]),  "type": 'accept', "aid": acceptor,
              "reg":register,
              "b": ballot_number, "v": new_state, "handled": 0}
        p = subprocess.run(['korni3', 'insert', self.korni3container, table],
                           input=json.dumps(data).encode('utf-8'))

    def send_accept(self, register, fId, ballot_number, state, nextValue):
        """
        7. Applies the f function to the current state and sends the result, new state, along with the generated ballot number B (an "accept" message) to the acceptors.
        """

        # transform state and save it
        f = getFunction(fId)
        state = f(state, nextValue)
        self._write(register, ballot_number, fId, nextValue, state)

        acceptations = []
        for acceptor in self.acceptors:
            #acceptation = acceptor.accept(ballot_number=ballot_number, new_state=self.state)
            #RPC
            self._sendAcceptRequest(acceptor, register, ballot_number, state)

        # then wait and then when response quorum , then accept2

    # TODO call this
    def accept2(self):
        # after wait accept confirmations
        #  to public result
        data={"id": str(uuid.uuid4()),  "type": 'accept', "aid": acceptor,
              "reg":register,
              "b": ballot_number, "v": new_state}
        p = subprocess.run(['korni3', 'insert', self.korni3container, 'PAXOS-CHANGE-RESULT'],
                           input=json.dumps(data).encode('utf-8'))

    def checkPrepareConfirmsHandler(self):
        reqTable = "REQ-PREPARE-Part-"+self.partitionId
        resTable = "RESP-PREPARE-Part-" + self.partitionId

        def confirms():
            self.cur.execute("""
select id as reg,  zT, zU as aid, b , resp
from \"""" +resTable+ """\"
--group by 
where 
	-- for me
	proposerId=?
	and b in (select b from (select register, b, max(zT) as zT from StateProposerLocal
				group by 
					register, b
				-- is me write
				having zU=?
			))
	and not handled
	order by zT desc
            """, (self.mePubKey, self.mePubKey))
            confirms = dict()
            for record in self.cur.fetchall():
                reg = record[0]
                b = record[3]
                if (reg,b) not in confirms:
                    confirms[(reg,b)] = [record]
                else:
                    confirms[(reg, b)].append(record)
            for reg,bFromConf in confirms:
                records = confirms[(reg, bFromConf)]
                b, fId, next, state, F = self._read(reg)
                if len(records)>F:
                    # we has достаточно confirms
                    confirmsList = []
                    aids = set()
                    for r in records:
                        resp = json.loads(r[4])
                        aid = r[2] # todo check AID
                        if aid not in aids:
                            aids.add(aid)
                            confirmsList.append(resp)
                    self.prepare2(reg, b, fId, confirmsList)
                #mark req as handled
                self.cur.execute('update "'+resTable+'"  set handled=1 where proposerId=? ' +
                                 'and b=? and id=?  ',
                                 (self.mePubKey, bFromConf, reg))
                self.cur.connection.commit()
        confirms()

    def checkAcceptConfirmsHandler(self):
        aresptable='RESP-ACCEPT-Part-'+self.partitionId
        self.cur.execute('select id, zT, breq, reg, acceptation from "'+aresptable+'" ' +
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
            b, fId, next, state, F = self._read(reg)
            # F - на момент запроса сохранена в базе
            # но на момент ответа Proposer может быть пересоздан с другим списком acceptors
            #  считаем что текущий список acceptors согласован и можно бы пересчитать данный F
            # ПОДУМАТЬ ОБ ЭТОМ!

            if len(infoList) >= F + 1:
                # число ответов достаточно чтобы вычислить результат и сообщить о нем
                def _publishResult(isConfirm: bool):
                    table = 'PAXOSRESULT-Part-'+self.partitionId
                    # TODO ид запроса, такой же взять как у запроса, для связи
                    if str(b) == str(breq):
                        data = {"id": str(uuid.uuid4()), "reg": reg, "breq": breq,
                                "fId": fId,
                                "bresult": b, "F": F,  #, "proposerId": self.mePubKey, - zU
                                "state": state if isConfirm else 'CONFLICT'}
                        # todo ? добавить в ответ все связанные документы с подписями ? или написать отдельный отчет?
                        # todo проверить перед созданием ответа, что он еще не существует. ? --ignore
                        subprocess.run(['korni3', 'insert', self.korni3container, table, '--ignore'],
                                            input=json.dumps(data).encode('utf-8'), capture_output=True)

                def _makrConfirmsAsHandled():
                    self.cur.execute('update "' + aresptable + '" set handled=1 where id=?',
                                     (json.dumps([reg, breq])))
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
                            self.cur.execute('update "' + aresptable + '" set handled=1 where ' +
                                             ' id=? and zT=?',
                                             (i["id"], i["zT"]))
                        self.cur.connection.commit()
                    _conflictRespAsHandled()





if __name__ == '__main__':
    # sure, is container are exists ?
    subprocess.run(['mkdir', '-p', '~/.config/korni/paxos/'])
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

    # TODO version
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

    acceptorsList = [mePub]
    # TODO create acceptors objects for range of partotions

    # todo make name a env param
    p = ProposerKorni3(acceptors=acceptorsList,
                       korni3container='paxos',
                       partitionId=partitionK1,
                       mePubKey=mePub)
    #print("p._sendPrepareRequest(mePub, 'k1', 10)", p._sendPrepareRequestUsingKorni3(mePub, 'k1', 10))
    p.receive('k1', 'change_func', 3)


    ####################### handle requests and confirm and messages founded in files
    def checkAcceptorsReq(acceptorsObects):
        for a in acceptorsObects:
            a.checkPrepareRequestsHandler()
            a.checkAcceptRequestsHandler()
    def checkPoposerRequests(proposerObject):
        proposerObject.checkPrepareConfirmsHandler()
        proposerObject.checkAcceptConfirmsHandler()

    # a1 - acceptor.   [a1] - acceptors list
    # p - proposer

    CHECK_PERIOD = datetime.timedelta(seconds=5) # sec  # TODO make it a env var
    while True:
        print('NEW LOOP')
        t1 = datetime.datetime.now()
        t2 = t1 + CHECK_PERIOD
        if True:
            # stuff jobs here
            print('Acceptors req\\res check')
            checkAcceptorsReq([a1]) # TODO MAKE it a param
            print('Proposers req\\res check')
            checkPoposerRequests(p)
            # jobs end
        pause.until(t2)

    #######################
    exit()
