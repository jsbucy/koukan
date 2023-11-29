
from router_service import Service

import logging

from pysmtpgw_config import Config as Wiring

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')

    service = Service()

    wiring=Wiring()
    service.main(wiring)
