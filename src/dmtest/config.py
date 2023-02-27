import tomllib as toml


def read_config(path):
    with open('config.toml', 'r') as f:
        config = toml.load(f)
        # FIXME: validate
        return config
