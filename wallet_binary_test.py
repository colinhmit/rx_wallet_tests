import random
import time
import datetime
import zmq
import multiprocessing
import threading
import uuid
import json
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
HotAccountMaster_config ={
    'input_host': '0.0.0.0',
    'input_port': 8000,

    'output_host': '0.0.0.0',
    'output_port': 8001,

    'master_port': 10000
}

class HotAccountMaster:
    def __init__(self, config, tiers):
        self.config = config
        self.tiers = tiers

        self.context = zmq.Context()
        self.set_sock()
        self.set_pipes()
        self.spawn()

        self.pending = []
        threading.Thread(target=self.listen).start()

        self.process()

    def set_sock(self):
        self.sock = self.context.socket(zmq.PULL)
        self.sock.connect('tcp://' +
                            self.config['input_host'] +
                            ':' +
                            str(self.config['input_port']))

    def set_pipes(self):
        self.out_pipe = self.context.socket(zmq.PUB)
        self.out_pipe.connect('tcp://' +
                          self.config['output_host'] +
                          ':' +
                          str(self.config['output_port']))

        self.under_pipe = self.context.socket(zmq.PUB)
        self.under_pipe.bind("tcp://*:" + str(self.config['master_port']))

    def listen(self):
        for account in iter(self.sock.recv_string, 'STOP'):
            self.pending.append(account)

    def process(self):
        while True:
            processing = self.pending[:2**self.tiers]
            self.pending = self.pending[2**self.tiers:]

            if len(processing) == 0:
                pass

            else:
                pp('Master processing')
                pp('Num processing: ' + str(len(processing)))
                pp('Num remaining: ' + str(len(self.pending)))
                if len(processing) == 1:
                        time.sleep(random.randint(5,10))
                        self.out_pipe.send_string(processing[0])

                else:
                    time.sleep(random.randint(5,10))
                    half = len(processing) / 2
                    self.under_pipe.send(str(self.left_port)+":"+json.dumps(processing[half:]))
                    self.under_pipe.send(str(self.right_port)+":"+json.dumps(processing[:half]))

    def spawn(self):
        sub_config = {
            'parent_host': '0.0.0.0',
            'parent_port': self.config['master_port'],
            'output_host': '0.0.0.0',
            'output_port': 8001,
            'tier': self.tiers-1
        }

        self.left_port = self.config['master_port'] + 1
        self.right_port = self.config['master_port'] + (2 ** self.tiers)
        multiprocessing.Process(target=HotAccountSub,args=(sub_config, self.left_port)).start()
        multiprocessing.Process(target=HotAccountSub,args=(sub_config, self.right_port)).start()

class HotAccountSub:
    def __init__(self, config, port):
        self.config = config
        self.port = port

        self.context = zmq.Context()
        self.set_sock()
        self.set_pipes()
        self.spawn()

        self.process()

    def set_sock(self):
        self.sock = self.context.socket(zmq.SUB)
        self.sock.connect('tcp://' +
                            self.config['parent_host'] +
                            ':' +
                            str(self.config['parent_port']))
        self.sock.setsockopt(zmq.SUBSCRIBE, str(self.port)+":")

    def set_pipes(self):
        self.out_pipe = self.context.socket(zmq.PUB)
        self.out_pipe.connect('tcp://' +
                          self.config['output_host'] +
                          ':' +
                          str(self.config['output_port']))

        self.under_pipe = self.context.socket(zmq.PUB)
        self.under_pipe.bind("tcp://*:" + str(self.port))

    def process(self):
        for data in iter(self.sock.recv, 'STOP'):
            pp('Tier: ' + str(self.config['tier']) + ' processing.')
            time.sleep(random.randint(5,10))
            data = json.loads(data[6:])

            if len(data) == 1:
                time.sleep(random.randint(5,10))
                self.out_pipe.send_string(data[0])

            else:
                time.sleep(random.randint(5,10))
                half = len(data) / 2
                self.under_pipe.send(str(self.left_port)+":"+json.dumps(data[half:]))
                self.under_pipe.send(str(self.right_port)+":"+json.dumps(data[:half]))

    def spawn(self):
        if self.config['tier'] > 0:
            sub_config = {
                'parent_host': '0.0.0.0',
                'parent_port': self.port,
                'output_host': '0.0.0.0',
                'output_port': 8001,
                'tier': self.config['tier']-1
            }
            self.left_port = self.port + 1
            self.right_port = self.port + (2 ** self.config['tier'])
            multiprocessing.Process(target=HotAccountSub,args=(sub_config, self.left_port)).start()
            multiprocessing.Process(target=HotAccountSub,args=(sub_config, self.right_port)).start()

client_master = ClientMaster(ClientMaster_config)

tiers = 3
multiprocessing.Process(target=HotAccountMaster,args=(HotAccountMaster_config, tiers)).start()

test_config = {
    'trial_time_seconds': 100,
    'reqs_per_second': 1
}

client_master.start_test(test_config)
