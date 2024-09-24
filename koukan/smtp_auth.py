# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from aiosmtpd.smtp import AuthResult, LoginPassword

import json

import secrets
import hashlib

# WARNING only suitable for high-entropy secrets, use bcrypt for user passwords!
class Authenticator:
    def __init__(self, path):
        f = open(path, "r")
        # [[username, hashed secret]]
        self.json = json.load(f)

    def __call__(self, server, session, envelope, mechanism, auth_data):
        print("Authenticator.__call__")
        fail_nothandled = AuthResult(success=False, handled=False)
        if mechanism not in ("LOGIN", "PLAIN"):
            return fail_nothandled
        if not isinstance(auth_data, LoginPassword):
            return fail_nothandled
        if not self.check(auth_data.login.decode('utf-8'),
                          auth_data.password.decode('utf-8')):
            return fail_nothandled
        return AuthResult(success=True)

    def check(self, user, passwd):
        if user not in self.json:
            return False  # unknown user
        if (hashlib.sha256(passwd.encode('utf-8')).hexdigest() !=
            self.json[user]):
            return False  # auth fail
        return True

def generate():
    s = secrets.token_urlsafe(16)
    print(s, hashlib.sha256(s.encode('utf-8')).hexdigest())

if __name__ == '__main__':
    generate()
