from typing import Any, Dict, Optional
from koukan.rest_schema import WhichJson

class Sender:
    name : Optional[str] = None
    tag : Optional[str] = None
    yaml : Optional[Dict[str,Any]] = None

    def __init__(self, name : Optional[str] = None,
                 tag : Optional[str] = None,
                 yaml : Optional[Dict[str,Any]] = None):
        self.name = name
        self.tag = tag
        self.yaml = yaml

    @staticmethod
    def from_json(js, which_js):
        return Sender(js.get('name', None),
                      js.get('tag', None),
                      js)

    def to_json(self, which_js):
        out = dict(self.yaml) if self.yaml else {}
        if self.name is not None:
            out['name'] = self.name
        if self.tag is not None:
            out['tag'] = self.tag
        return out

    def copy(self, w : WhichJson = WhichJson.ALL):
        return Sender(self.name, self.tag,
                      dict(self.yaml) if self.yaml is not None else None)

    def __repr__(self):
        return '%s %s' % (self.name, self.tag)

    def __eq__(self, rhs):
        return self.name == rhs.name and self.tag == rhs.tag
