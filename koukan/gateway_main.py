# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import logging
import sys

from koukan.gateway import SmtpGateway

def main(argv):
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(process)d] [%(thread)d] '
        '%(filename)s:%(lineno)d %(message)s')
    gw = SmtpGateway()
    gw.main(argv)

if __name__ == '__main__':
    try:
        main(sys.argv)
    except SystemExit:
        logging.exception('exit')
