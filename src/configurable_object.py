class ConfigError(Exception):
    pass

class ConfigurableObject:
    def __init__(self, **kwargs):
        object.__setattr__(self, '_config', kwargs.copy())
        self.check_config(self._config)

    def check_config(self):
        pass

    def __getattr__(self, attr):
        try:
            config = object.__getattr__(self, attr)
        except AttributeError:
            object.__getattr__(self, attr)
            return
        if attr in config:
            return config[attr]
        else:
            object.__getattr__(self, attr)

    def __setattr__(self, attr, value):
        try:
            config = object.__getattr__(self, attr)
        except AttributeError:
            object.__setattr__(self, attr, value)
            return
        if attr in config:
            config[attr] = value
            self.check_config(config)
        else:
            object.__setattr__(self, attr, value)

    def get_config(self, **kwargs):
        result = self._config.copy()
        for key, value in kwargs.items():
            if key in result:
                result[key] = value
            else:
                raise ConfigError('Object has no config option {0:s}' \
                                  .format(repr(key)))
        self.check_config(result)
        return result

    def set_config(self, **kwargs):
        for key, value in kwargs.items():
            if key in self._config:
                self._config[key] = value
            else:
                raise ConfigError('Object has no config option {0:s}' \
                                  .format(repr(key)))
        self.check_config(self._config)

    def add_config(self, **kwargs):
        for key, value in kwargs.items():
            if key in self._config:
                raise ConfigError('Config option `{0:s}` already present.' \
                                  .format(key))
            else:
                self._config[key] = value

        self.check_config(self._config)

