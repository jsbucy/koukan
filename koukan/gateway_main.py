import logging
from koukan.gateway import SmtpGateway

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(process)d] [%(thread)d] '
        '%(filename)s:%(lineno)d %(message)s')

    gw = SmtpGateway()
    gw.main()
