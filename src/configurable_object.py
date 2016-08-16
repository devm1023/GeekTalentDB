class ConfigError(Exception):
    pass

class ConfigurableObject:
    def __init__(self, **kwargs):
        if hasattr(self, '_config'):
            self._config.update(kwargs)
        else:
            object.__setattr__(self, '_config', kwargs.copy())
        self.check_config(self._config)

    def check_config(self, config):
        pass

    def __getattr__(self, attr):
        if attr != '_config' and attr in self._config:
            return self._config[attr]
        else:
            raise AttributeError('{0:s} object has no attribute {1:s}' \
                                 .format(repr(self.__class__.__name__),
                                         repr(attr)))

    def __setattr__(self, attr, value):
        if hasattr(self, '_config') and attr in self._config:
            self._config[attr] = value
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


if __name__ == '__main__':
    obj = ConfigurableObject(a=1, b=2)
    obj.a = 3
    obj.x = 4
    print(obj.a)
    print(obj.b)
    print(obj.x)
    print(obj.get_config(b=10))
    
