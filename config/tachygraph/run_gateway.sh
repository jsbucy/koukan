capsh --keep=1 --user=rangifer --caps='cap_net_bind_service+pei cap_setpcap,cap\
_setuid,cap_setgid+ep' --addamb=cap_net_bind_service -- -c "PYTHONPATH=/home/bu\
cy/pysmtpgw python3 /home/bucy/pysmtpgw/gateway.py 8001 8000 25 587 \
 /etc/letsencrypt/live/tachygraph.gloop.org/fullchain.pem \
 /etc/letsencrypt/live/tachygraph.gloop.org/privkey.pem \
 secrets.json tachygraph.gloop.org"
