import asyncio
import datetime
# import pause
from paxoskornilibclient import CASPaxosClient


async def main():

    req1id, f1 = CASPaxosClient.paxosKorni3ClientRequest('k1', 'read_func', 'NULL')
    def next1f1(data):
        print('data=', data, self, f1, f1.result(), f1.done)
    f1.add_done_callback(next1f1)

    req2id, f2 = CASPaxosClient.paxosKorni3ClientRequest('k1', 'cas_version_func', {"version": 1, "value": 123})
    # paxosKorni3ClientCheckResult = CASPaxosClient.paxosKorni3GetChecker('paxos')
    f2.add_done_callback(next1f1)

    # CHECK_PERIOD = datetime.timedelta(seconds=3)
    # while True:
    #     # t1 = datetime.datetime.now()
    #     # t2 = t1 + CHECK_PERIOD
    #     if True:
    #         # Get the current event loop.
    #         loop = asyncio.get_running_loop()
    #
    #         # v = paxosKorni3ClientCheckResult(req1id, 'k1')
    #         # if not v is None : print('v=', v)
    #         # v = paxosKorni3ClientCheckResult(req2id, 'k1')
    #         # if not v is None : print('v=', v)
    #
    #     # pause.until(t2)
    #     # await asyncio.sleep(3)
    #
    await f1
    print('f1, result=', f1.result())
    await f2
    print('f2, result=', f2.result())
    await f1, f2
    print('done, bay')


if __name__ == "__main__":
    import time
    s = time.perf_counter()
    # asyncio.run(main())
    asyncio.get_event_loop().run_until_complete(main())
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")