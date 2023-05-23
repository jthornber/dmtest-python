import dmtest.config as config


class Fixture:

    def __str__(self):
        return str(self._cfg)

    def __init__(self):
        self._cfg = config.read_config()

    @property
    def cfg(self):
        return self._cfg
