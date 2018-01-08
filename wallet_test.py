import random
import time
import datetime
import zmq
import multiprocessing
import threading
import uuid
import numpy

# logging
red = "\033[01;31m{0}\033[00m"


def pp(message, mtype='INFO'):
    mtype = mtype.upper()
    if mtype == "ERROR":
        mtype = red.format(mtype)
    print '[%s] [%s] %s' % (time.strftime('%H:%M:%S', time.gmtime()),
                            mtype,
                            message)

# agg clients
ClientMaster_config = {
    'input_port': 8000,
    'output_port': 8001
}

class ClientMaster:
    def __init__(self, config):
        self.config = config
        self.context = zmq.Context(1)
        self.set_sock()
        self.set_pipe()

        self.pending_withdrawals = {}

    def start_test(self, test_config):
        self.test_config = test_config
        threading.Thread(target=self.listen).start()
        threading.Thread(target=self.request).start()
        threading.Thread(target=self.monitor).start()

    def set_sock(self):
        self.sock = self.context.socket(zmq.SUB)
        self.sock.bind("tcp://*:" + str(self.config['output_port']))
        self.sock.setsockopt(zmq.SUBSCRIBE, "")

    def set_pipe(self):
        self.pipe = self.context.socket(zmq.PUSH)
        self.pipe.bind("tcp://*:" + str(self.config['input_port']))

    def request(self):
        total_reqs = self.test_config['reqs_per_second'] * self.test_config['trial_time_seconds']

        while total_reqs > 0:
            account = str(uuid.uuid1())
            pp('Sending request for account: ' + account)
            self.pending_withdrawals[account] = datetime.datetime.now()
            self.pipe.send_string(account)
            total_reqs -= 1

            time.sleep(1.0 / self.test_config['reqs_per_second'])

    def listen(self):
        for account in iter(self.sock.recv_string, 'STOP'):
            if account in self.pending_withdrawals:
                pp('Received for account: ' +
                    account +
                    ', after: ' +
                    str((datetime.datetime.now()-self.pending_withdrawals[account]).total_seconds()))
                del self.pending_withdrawals[account]
            else:
                pp('UNRECOGNIZED ACCOUNT: ' + account)

    def monitor(self):
        time.sleep(10)
        while True:
            if len(self.pending_withdrawals) > 0:
                num_accounts = len(self.pending_withdrawals)
                waiting_times = []

                for account in self.pending_withdrawals:
                    waiting_times.append(
                        (datetime.datetime.now() - self.pending_withdrawals[account]).total_seconds()
                    )

                pp('////Pending Withdrawal Stats////')
                pp('Number of accounts waiting: ' + str(num_accounts))
                pp('Avg time in queue: ' + str(numpy.mean(waiting_times)))
                pp('Max time in queue: ' + str(max(waiting_times)))
                pp('///////////////////////////////')
            else:
                pp('//// NO PENDING WITHDRAWALS ////')
            time.sleep(5)

#hot accounts
HotAccount_config ={
    'input_host': '0.0.0.0',
    'input_port': 8000,

    'output_host': '0.0.0.0',
    'output_port': 8001
}

class HotAccount:
    def __init__(self, config):
        self.config = config

        self.context = zmq.Context()
        self.set_sock()
        self.set_pipe()

        self.process()

    def set_sock(self):
        self.sock = self.context.socket(zmq.PULL)
        self.sock.connect('tcp://' +
                            self.config['input_host'] +
                            ':' +
                            str(self.config['input_port']))

    def set_pipe(self):
        self.pipe = self.context.socket(zmq.PUB)
        self.pipe.connect('tcp://' +
                          self.config['output_host'] +
                          ':' +
                          str(self.config['output_port']))

    def process(self):
        for account in iter(self.sock.recv_string, 'STOP'):
            time.sleep(random.randint(5,10))
            self.pipe.send_string(account)

client_master = ClientMaster(ClientMaster_config)

workers = []
num_workers = 10
for _ in xrange(num_workers):
    workers.append(multiprocessing.Process(target=HotAccount,args=(HotAccount_config,)).start())


test_config = {
    'trial_time_seconds': 100,
    'reqs_per_second': 1
}

client_master.start_test(test_config)
