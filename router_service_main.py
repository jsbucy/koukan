from router_service import Service
import sys

import logging

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')

    service = Service()

    service.main(sys.argv[1])
