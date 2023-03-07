import toml


def read_config(path="config.toml"):
    with open(path, "r") as f:
        config = toml.load(f)
        # FIXME: validate
        return config
