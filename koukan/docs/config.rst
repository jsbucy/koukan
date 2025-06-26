=============
Configuration
=============

Endpoints/Output Chains
=======================
You will typically have 1 endpoint for each smtp vip + port e.g. mx and msa.
Any endpoint that terminates smtp will end with the exploder.
There will be an accompanying "exploder upstream" endpoint/chain
with "per_request" retry/notify settings.
There will also be an endpoint for direct rest clients.
So a simple config might consist of::

    endpoint:
    - name: smtp-mx
      chain:
      # ...
      - filter: exploder
        msa: false
        output-chain: smtp-mx-upstream
    - name: smtp-mx-upstream
      output_handler:
        notification:
          host: submission
        retry_params:
      chain:
      # ...
      - filter: rest_output

msa is similar to mx but enables store&forward on the exploder in more
cases with ``msa: true``

rest clients that take full advantage of RestMTP LROs don't need notifications::

    - name: submission
      output_handler:
        retry_params:
      chain:
      # ...
      - filter: rest_output


Note that the output chain is linear. Koukan routes on recipient by
setting fields in the transaction to influence the next hop that
rest_output sends to and if that is the smtp gateway, what destination
the gateway sends to after that. A typical ingress config would route
known domains and reject everything else. Whereas an egress config
might special-case internal domains and then send everything else to
the RHS of the address.


cluster/k8s
===========
