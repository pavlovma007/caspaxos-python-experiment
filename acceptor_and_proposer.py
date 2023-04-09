import datetime
import json
import subprocess
from subprocess import PIPE
from pathlib import Path
import os
import pause

from caspaxos_lib import AcceptorKorni3, ProposerKorni3, partitionFromIndex


def _checkAcceptorsReq(acceptorsObects):
    for a in acceptorsObects:
        a.checkPrepareRequestsHandler()
        a.checkAcceptRequestsHandler()


def _checkPoposerRequests(proposerObject):
    proposerObject.checkInputRequest()
    proposerObject.checkPrepareConfirmsHandler_RESP_PREPARE()
    proposerObject.checkAcceptConfirmsHandler()


if __name__ == '__main__':
    print('starting...')
    checkperiod = int(os.environ['CHECK_PERIOD']) if 'CHECK_PERIOD' in os.environ else 3 #  sec
    contName = os.environ['PAXOSKORNICONTAINER'] if 'PAXOSKORNICONTAINER' in os.environ else 'paxos'
    # sure, is container are exists ?
    subprocess.run(['mkdir', '-p', str(Path.home())+'/.config/korni/paxos/'])
    # who i am ?
    p = subprocess.run(['bash', '-c', "korni3 login   `cat ~/.config/korni/MY-KEYS/selected`| awk '{print $2}' "],
                   stdout=PIPE, stderr=PIPE)
    mePub = p.stdout.decode('utf-8')

    acceptorPartitions = json.loads(os.environ['PAXOSACCEPTORPARTITIONS']) if 'PAXOSACCEPTORPARTITIONS' in os.environ else []
    acceptors = [AcceptorKorni3(partitionFromIndex(part), contName, mePub) for part in acceptorPartitions]
    proposerPartitions = json.loads(os.environ['PAXOSPROPOSERPARTITIONS']) if 'PAXOSPROPOSERPARTITIONS' in os.environ else []
    # TODO config WHO is acceptors for partition ?
    proposers = []
    #proposers = [ProposerKorni3(acceptors, contName, partitionFromIndex(part), mePub) for part in proposerPartitions]

    if not (len(acceptors) == 0 and len(proposers) == 0):
        print('working...')
        CHECK_PERIOD = datetime.timedelta(seconds=checkperiod)
        while True:
            t1 = datetime.datetime.now()
            t2 = t1 + CHECK_PERIOD
            if True:
                # stuff jobs here

                _checkAcceptorsReq(acceptors)
                #_checkPoposerRequests(p)

                # jobs end
            pause.until(t2)
