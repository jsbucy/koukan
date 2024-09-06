from router_service import Service
import sys

import logging

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(process)d] [%(thread)d] %(message)s')
    #logging.getLogger('httpx').setLevel(logging.INFO)
    #logging.getLogger('httpcore').setLevel(logging.INFO)
    logging.getLogger('hpack').setLevel(logging.INFO)

    service = Service()

    service.main(sys.argv[1])
