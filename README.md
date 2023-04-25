# Installation

- Make sure python3 is installed.
- Make sure pip is installed.
- Install python deps:

> pip install -r requirements.txt

- Edit config.toml for your system
- Check for tool requirements by running:

> ./dmtest health

In addition many of the tests perform operations on a copy of
linux git repository:

> git clone https://github.com/torvalds/linux.git


# Running

## List tests

> ./dmtest list --rx <regex>


## Run tests

> ./dmtest run --rx <regex>

