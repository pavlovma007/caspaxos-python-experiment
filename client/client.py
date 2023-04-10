import time
import asyncio
from paxoskorniclientlib import CASPaxosClient


async def main():

    req1id, f1 = CASPaxosClient.paxosKorni3ClientRequest('k1', 'read_func', 'NULL')
    def next1f1(data):
        print('data=', data, f1, f1.result(), f1.done)
    f1.add_done_callback(next1f1)
    print('req1id', req1id)

    req2id, f2 = CASPaxosClient.paxosKorni3ClientRequest('k1', 'cas_version_func', {"version": 1, "value": 123})
    print('req2id', req2id)
    # paxosKorni3ClientCheckResult = CASPaxosClient.paxosKorni3GetChecker('paxos')
    def next1f2(data):
        print('data=', data, f1, f1.result(), f1.done)
    f2.add_done_callback(next1f2)


    await(f1)
    print('f1, result=', f1.result())
    await(f2)
    print('f2, result=', f2.result())
    await(f1, f2)
    print('done, bay')


if __name__ == "__main__":
    s = time.perf_counter()
    # asyncio.run(main())
    asyncio.get_event_loop().run_until_complete(main())
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")