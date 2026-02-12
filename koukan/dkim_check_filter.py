# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Dict, List, Optional
from enum import IntEnum
import logging

import dkim
import dkim.util
import publicsuffix2

from koukan.address import domain_from_address
from koukan.filter import TransactionMetadata
from koukan.filter_chain import Filter, FilterResult
from koukan.filter_output import FilterOutput
from koukan.matcher_result import MatcherResult
from koukan.response import Response
from koukan.rest_schema import WhichJson

from koukan.message_validation_filter import MessageValidationFilter

class Status(IntEnum):
    temp_err = 0  # e.g. dns
    dkim_pass = 1
    fail = 2
    unknown_algo = 3


# header-from domain vs d= "sdid"
class Alignment(IntEnum):
    domain = 0
    same_sld = 1  # publicsuffix2.get_sld() a la dmarc relaxed alignment
    other = 2

class DkimCheckFilterOutput(FilterOutput):

    # for each dkim-signature header
    class Result:
        domain : Optional[str] = None  # d= sdid
        status : Status = Status.fail
        alignment : Optional[Alignment] = None

        timestamp : Optional[int] = None
        expiration : Optional[int] = None
        headers : List[str]
        tags : Dict[str,str]
        def __init__(self):
            self.tags = {}
            self.headers = []

        def to_json(self):
            out = { 'status': self.status.name }
            for field in ('alignment', 'headers', 'timestamp', 'expiration',
                          'signing_domain', 'tags'):
                if hasattr(self, field) and (v := getattr(self, field)):
                    out[field] = v
            return out

    results: List[Result]

    def __init__(self):
        self.results = []

    def match(self, yaml : dict):
        assert not ('alignment' in yaml and 'domains' in yaml)
        align = Alignment[yaml.get('alignment', 'same_sld')]
        status = Status[yaml.get('status', 'dkim_pass')]
        domains = yaml.get('domains', [])
        for r in self.results:
            if r.status != status:
                continue
            if domains:
                for d in domains:
                    if d == r.domain:  # xxx case?
                        return MatcherResult.MATCH
                return MatcherResult.NO_MATCH
            # TODO ever want exact alignment?
            if r.alignment is None or (r.alignment > align):
                continue
            return MatcherResult.MATCH

        return MatcherResult.NO_MATCH

    def to_json(self, w : WhichJson):
        if w not in [WhichJson.DB_ATTEMPT,
                     WhichJson.REST_CREATE,
                     WhichJson.REST_UPDATE]:
            return None
        if self.results is None:
            return None
        return {'results': [r.to_json() for r in self.results]}

class DkimCheckFilter(Filter):
    def __init__(self, inject_dns=None):
        self.inject_dns = inject_dns

    def _fixup_tags(self, header_value : bytes,
                    result : DkimCheckFilterOutput.Result):
        result.tags = {
            k.decode('ascii') : v.decode('ascii')
            for k,v in dkim.util.parse_tag_value(header_value).items()}
        # xxx decode error, domain could be utf8?
        if headers := result.tags.get('h', None):
            result.headers = [h.strip() for h in headers.split(':')]
            del result.tags['h']
        if domain := result.tags.get('d', None):
            result.domain = domain
            del result.tags['d']

        for tag,field in [('t','timestamp'), ('x','expiration')]:
            if (value := result.tags.get(tag, None)):
                if value.isdigit():
                    setattr(result, field, int(value))
                    del result.tags[tag]

        # drop these because they're large and unlikely to be
        # useful to matchers
        for h in ['b', 'bh']:
            if h in result.tags:
                del result.tags[h]

    def on_update(self, tx_delta : TransactionMetadata) -> FilterResult:
        if (body := tx_delta.maybe_body_blob()) is None or not body.finalized():
            return FilterResult()

        tx = self.downstream_tx
        assert tx is not None

        b = body.pread(0)
        verifier = dkim.DKIM(b)

        out = tx.get_filter_output(self.fullname())
        if out is None:
            out = DkimCheckFilterOutput()
            tx.add_filter_output(self.fullname(), out)

        i = 0
        for k,v in verifier.headers:
            if k.lower() != b'dkim-signature':
                continue
            ex = None
            res = DkimCheckFilterOutput.Result()
            signer_domain = None
            try:
                self._fixup_tags(v, res)
                kwargs = {}
                if self.inject_dns:
                    kwargs['dnsfunc'] = self.inject_dns
                s = verifier.verify(i, **kwargs)
                status = Status.dkim_pass if s is True else Status.fail
                logging.debug(status)
            except dkim.ValidationError as e:
                logging.debug(e)
                ex = e
                if 'unknown signature algorithm' in str(e):
                    status = Status.unknown_algo
                else:
                    status = Status.fail
            except Exception as e:
                logging.debug(e)
                ex = e
                status = Status.temp_err

            valid = tx.get_filter_output(MessageValidationFilter.fullname())
            header_from_domain = valid.parsed_header_from.domain
            logging.debug('%s %s', header_from_domain, res.domain)
            if header_from_domain == res.domain:  # xxx case?
                res.alignment = Alignment.domain
            else:
                header_from_sld = publicsuffix2.get_sld(header_from_domain)
                signer_sld = publicsuffix2.get_sld(res.domain)
                logging.debug('%s %s', header_from_sld, signer_sld)
                if header_from_sld == signer_sld:  # xxx case?
                    res.alignment = Alignment.same_sld
                else:
                    res.alignment = Alignment.other

            res.status = status
            out.results.append(res)
            i += 1

        return FilterResult()
