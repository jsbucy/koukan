
import gunicorn.app.base

class StandaloneApplication(gunicorn.app.base.BaseApplication):
    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        config = {key: value for key, value in self.options.items()
                  if key in self.cfg.settings and value is not None}
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application

def run(host, port, cert, key, app):
    options = {
        'bind': '%s:%s' % (host, port),
        'threads': 4,
        'worker-class': 'gthread'
    }
    if cert and key:
        options['certfile'] = cert
        options['keyfile'] = key
    StandaloneApplication(app, options).run()
