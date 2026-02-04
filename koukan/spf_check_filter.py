# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, List, Optional
from enum import IntEnum

import spf
import logging

from koukan.address import domain_from_address
from koukan.filter import TransactionMetadata
from koukan.filter_chain import Filter, FilterResult
from koukan.filter_output import FilterOutput
from koukan.matcher_result import MatcherResult
from koukan.response import Response

class SpfCheckFilterOutput(FilterOutput):
    class Status(IntEnum):
        temperror = 0
        spf_pass = 1
        permerror = 2
        fail = 3
        softfail = 4
        none = 5
        neutral = 6

        @staticmethod
        def from_str(s):
            if s == 'pass':
                s = 'spf_pass'
            return SpfCheckFilterOutput.Status[s]

    mail_from_result : Optional[Status] = None

    # some inbound gateway setups (e.g. google apps/gsuite/workspace) use spf to
    # enumerate their egress IPs

    extra_domains_results : Dict[str, Status]
    def __init__(self):
        self.extra_domains_results = {}

    def match(self, yaml : dict):
        # This never returns PRECONDITION_UNMET: this should be
        # invoked from a chain immediately upstream from smtp so the
        # first tx update always includes remote_host, smtp_meta and
        # mail_from so the spf results should always be populated
        # albeit possibly with an error
        if expected_mail_from := yaml.get('mail_from_result', None):
            assert self.mail_from_result is not None
            if (self.mail_from_result !=
                SpfCheckFilterOutput.Status.from_str(expected_mail_from)):
                return MatcherResult.NO_MATCH
        if ((extra_domain := yaml.get('extra_domain', None)) and
            (expected_result := yaml.get('extra_domain_result', None))):
            domain_result = self.extra_domains_results.get(extra_domain, None)
            assert domain_result is not None
            if (domain_result !=
                SpfCheckFilterOutput.Status.from_str(expected_result)):
                return MatcherResult.NO_MATCH
        return MatcherResult.MATCH

class SpfCheckFilter(Filter):
    extra_domains : List[str]
    def __init__(self, extra_domains : List[str]):
        self.extra_domains = extra_domains

    def on_update(self, tx_delta : TransactionMetadata) -> FilterResult:
        if tx_delta.remote_host is None and tx_delta.mail_from is None:
            return FilterResult()

        tx = self.downstream_tx
        assert tx is not None
        out = tx.get_filter_output(self.fullname)
        if out is None:
            out = SpfCheckFilterOutput()
            tx.add_filter_output(self.fullname, out)

        if tx_delta.remote_host is not None:
            for d in self.extra_domains:
                # we're passing a domain from config here so it should
                # never fall back to ehlo.
                out.extra_domains_results[d] = self._check(d, ehlo=None)

        if tx_delta.mail_from is not None or tx_delta.smtp_meta is not None:
            # Checking the ehlo domain is (only?) for <>/bounces.
            ehlo = None
            if tx.smtp_meta:
                ehlo = tx.smtp_meta.get('ehlo', None)

            env_from_domain = ''
            if tx_delta.mail_from and tx_delta.mail_from.mailbox:
                # pyspf appears to split mail_from on @ which is
                # incorrect for quoted-string local part. Academic but
                # domain_from_address() probably does a better job
                # with email._header_value_parser.
                env_from_domain = domain_from_address(
                    tx_delta.mail_from.mailbox)
            status = self._check(env_from_domain, ehlo)
            out.mail_from_result = status
        return FilterResult()

    def _check(self, domain, ehlo):
        tx = self.downstream_tx
        assert tx is not None

        assert tx.remote_host is not None
        host = tx.remote_host.host

        result, detail = spf.check2(i=host, s=domain, h=ehlo)
        status = SpfCheckFilterOutput.Status.from_str(result)
        return status
