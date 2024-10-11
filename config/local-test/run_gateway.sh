PYTHONPATH=. exec python3 koukan/gateway_main.py config/local-test/gateway.yaml

# to listen on privileged ports:
# https://stackoverflow.com/questions/413807/is-there-a-way-for-non-root-processes-to-bind-to-privileged-ports-on-linux
# capsh --keep=1 --user=my-user --caps='cap_net_bind_service+pei cap_setpcap,cap_setuid,cap_setgid+ep' --addamb=cap_net_bind_service -- -c "PYTHONPATH=/home/my-user/koukan python3 /home/my-user/koukan/koukan/gateway_main.py /home/my-user/koukan/config/examples/local-test/tachygraph/gateway.yaml"
