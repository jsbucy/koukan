# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from koukan.router_service import Service
import sys

import logging

def main(argv):
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(process)d] [%(thread)d] '
        '%(filename)s:%(lineno)d %(message)s')

    service = Service()

    service.main(argv[1])

if __name__ == '__main__':
    try:
        main(sys.argv)
    except SystemExit:
        logging.exception('exit')
