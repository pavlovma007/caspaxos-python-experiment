import subprocess
import pause, datetime

from node.caspaxos_lib import partitionFromRegKey, AcceptorKorni3, ProposerKorni3

#################################### CLIENT API


if __name__ == '__main__':
    from client.paxoskorniclientlib import CASPaxosClient
    from pathlib import Path

    # sure, is container are exists ?
    subprocess.run(['mkdir', '-p', str(Path.home())+'/.config/korni/paxos/'])
    mePub = 'MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAvJQG4135BoAhgw+/X+11XOqj2d3BVSX61pLb7EKiPUirzoNo15nP/5ZFrGB00vTfiuGbkP/puBDXMiNnNQNKVHxJ8Ltu/W1Fxo9k5UiFvQ5mmml+ZolkqzKgHaDTM9a9OQ96mc4oelSsX81Nh5+5hsP8RmUHZ8A/pImqr3Ttjmx4BBz+m9fccgvMi2cjG/a79k5/Lv9AYO28MHImHt0whzzHyrKWGeLxEdyPVRrfgbp/crw0lQJsvWZUTkiKFSK8ur0qFvg7szhu+8AOIDcTV9me/uZgOAeEiohn8/KN8rkAlgdFVBmWyyO/KOHR2OK7+PeQDOIxn33xQ0KE9g5aN6lHgRljewsWieP1PNAH6KZCszSykIvdYD5fvniGrOxxJN4HI0CY9q/QWKHYxUzckXtuYuLGiXT5/FAyPdefUGVBt8oB2Nq/kWH4qgu325fdpj0HWYwSPIJWAwQL0/c/XzlJwEjScR+pw/CoMtw1IWDWd5Xx/a8N9hz6FtUiPVNyq8fVjiLwc8cRgo2SrVA2EV4yPRzNcBDGb+r1h5k1dNhc1ATGEJQUWG7zOa4Jj7m0xWoDBIuGFctt6gaIUYZyTHwecIQIjj0nvefW/4reXMGvjBhM+91qqq4zRVI9qWdftJXuE161YZbEWN5xfm3dfCa8udq5m3zEu7HktSlKBD0CAwEAAQ=='

    partitionK1 = partitionFromRegKey('k1')
    print('partitionK1', partitionK1)
    partitionK2 = partitionFromRegKey('k2')

    a1 = AcceptorKorni3(partition=partitionK1, korni3container='paxos', zU=mePub)

    acceptorsList = [mePub]
    # TODO create acceptors objects for range of partitions

    # todo make name a env param
    p = ProposerKorni3(acceptors=acceptorsList,
                       korni3container='paxos',
                       partitionId=partitionK1,
                       mePubKey=mePub)
    req1id = CASPaxosClient.paxosKorni3ClientRequest('k1', 'read_func', 'NULL')
    req2id = CASPaxosClient.paxosKorni3ClientRequest('k1', 'cas_version_func', {"version": 1, "value": 123} )
    paxosKorni3ClientCheckResult = CASPaxosClient._paxosKorni3GetChecker('paxos')

    ####################### handle requests and confirm and messages founded in files
    def _checkAcceptorsReq(acceptorsObects):
        for a in acceptorsObects:
            a.checkPrepareRequestsHandler()
            a.checkAcceptRequestsHandler()
    def _checkPoposerRequests(proposerObject):
        proposerObject.checkInputRequest()
        proposerObject.checkPrepareConfirmsHandler()
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
            v = paxosKorni3ClientCheckResult(req2id, 'k1')
            if not v is None : print('v=', v)
            # jobs end
        pause.until(t2)
    exit()
    #######################

