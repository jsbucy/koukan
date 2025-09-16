================
Extending Koukan
================

router yaml::

  modules:
    sync_filter:
      my_filter: path.to.my.filter
    recipient_router_policy:
      my_policy: path.to.my.policy:my_policy_factory

will do something like::

  from path.to.my.filter import factory
  filter = factory(filter_yaml)
  from path.to.my.policy import my_policy_factory
  policy = my_policy_factory(policy_yaml)

Filters
=======

Filter: returns FilterResult with a delta to be unconditionally merged
into the upstream result. This enables a sort-of tail-call
optimization; the filter is not invoked in the return path.

ProxyFilter: whereas a regular Filter only conservatively extends the
transaction, ProxyFilter has separate upstream and downstream
transaction objects that it can implement an arbitrary transformation
between. e.g. any filter that replaces/modifies tx.body (message
builder, message parser, received header) must be ProxyFilter.

CoroutineFilter: whereas regular Filter is not invoked in the return
path, CoroutineFilters take a Callable to "yield to the scheduler" to
continue the upstream chain which in turn yields a continuation to
invoke in the return path. This allows arbitrary code in the return
path. Historical note: we started with CoroutineFilter, then
discovered the tail-call optimization was possible for filters in a
"normal form" and then discovered that all of the filters fit into
that. So we don't actually have a use case for this currently.

CoroutineProxyFilter: ProxyFilter + CoroutineFilter


RecipientRouterFilter Policies
==============================

Implement  ``recipient_router_filter.RoutingPolicy``

``RoutingPolicy.endpoint_for_rcpt()`` is passed an element from
``TransactionMetadata.rcpt_to`` and returns either a ``Destination``
which controls where RestEndpoint sends it or a Response which is
returned downstream in ``TransactionMetadata.rcpt_response`` if the
address does not exist.
