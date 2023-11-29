from enum import IntEnum
from typing import Dict

class Status(IntEnum):
    INSERT = 0  # uncommitted
    WAITING = 1
    INFLIGHT = 2
    DONE = 3
    ONESHOT_INFLIGHT = 5
    ONESHOT_TEMP = 6  # set_durable -> WAITING

# TODO it seems like these fine-grained codes delivered/temp/perm are
# redundant with the resp, this is a vestige of not originally storing
# the resp? does anything directly reading the db distinguish those?
# or do you want it to capture things like upstream temp + max retry
# -> perm? or maybe that should be an additional action? conceptually
# that is a separate attempt LOAD, TEMP, LOAD, PERM though in practice
# the completion logic writes that?
class Action(IntEnum):
    INSERT = 0
    LOAD = 1  # WAITING -> INFLIGHT
              # INSERT -> ONESHOT_INFLIGHT
    RECOVER = 5  # INFLIGHT w/stale session -> WAITING
    DELIVERED = 2
    TEMP_FAIL = 3
    PERM_FAIL = 4
    ABORT = 6
    # START is basically upstream start succeeded, upstream start err ->
    # overall tx "attempt" result
    START = 7  # inflight -> inflight (no change)
    SET_DURABLE = 8  # INSERT -> WAITING
                     # ONESHOT_INFLIGHT -> INFLIGHT
    # we don't persist this action but it's used with exceptions
    APPEND = 9


# state -> action -> new state
# Dict[Status, Dict[Action, Status]]

transition : Dict[Status, Dict[Action, Status]] = {
    Status.INSERT: {
        Action.SET_DURABLE: Status.WAITING,
        Action.LOAD: Status.ONESHOT_INFLIGHT,
        Action.ABORT: Status.DONE },

    Status.ONESHOT_INFLIGHT: {
        Action.DELIVERED: Status.DONE,
        Action.PERM_FAIL: Status.DONE,
        Action.ABORT:  Status.DONE,
        Action.TEMP_FAIL:  Status.ONESHOT_TEMP,
        Action.START: Status.ONESHOT_INFLIGHT,  # no change
        Action.SET_DURABLE: Status.INFLIGHT },

    Status.ONESHOT_TEMP: {
        Action.SET_DURABLE: Status.WAITING,
        Action.ABORT: Status.DONE },

    Status.WAITING: {
        Action.LOAD: Status.INFLIGHT },

    # this could be a noop if it already succeeded
    # upstream but treat as failed precondition at storage
    # layer, deal with that in the handler
    Status.DONE: {},

    Status.INFLIGHT: {
        Action.DELIVERED: Status.DONE,
        Action.PERM_FAIL: Action.ABORT,
        Action.START: Status.INFLIGHT,  # no change
        Action.TEMP_FAIL: Status.WAITING },
}

class InvalidActionException(Exception):
    def __init__(self, status : Status, action : Action):
        pass


def check_valid_append(status : Status):
    if status not in [ Status.INSERT,
                       Status.ONESHOT_INFLIGHT,
                       Status.ONESHOT_TEMP ]:
        raise InvalidActionException(status, Action.APPEND)
