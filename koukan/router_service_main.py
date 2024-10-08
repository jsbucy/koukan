# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from koukan.router_service import Service
import sys

import logging

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(process)d] [%(thread)d] '
        '%(filename)s:%(lineno)d %(message)s')

    service = Service()

    service.main(sys.argv[1])
