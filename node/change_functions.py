# version CAS
def cas_version_func(state, nextValue):
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
        return cas_version_func({"version": 0, "value": 0}, nextValue)

    if type(state) == dict and 'version' in state and (nextValue['version'] == state['version'] + 1):
        return nextValue
    else:
        return state


def read_func(state, nextValue):
    return state


# def set_func(state, nextValue):
#     return nextValue

# export
def getFunction(id: str):
    if id == 'cas_version_func':
        return cas_version_func
    if id == 'read_func':
        return read_func
    # if id == 'set_func':
    #     return set_func

