# rx_wallet_tests

dependencies: zmq, numpy

wallet_test: tests pushpull worker funnel structure. See num_workers and the test_config to set up the load test. Stronger request and PoW randomization can be added.

    Request timing: no randomization
    line 62: time.sleep(1.0 / self.test_config['reqs_per_second'])

    PoW timing: weak integer randomization
    line 131: time.sleep(random.randint(5,10))

binary_wallet_test: binary tree structure. Does not result in any efficiency gain.
