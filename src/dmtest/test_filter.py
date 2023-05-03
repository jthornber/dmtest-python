import re
from abc import ABC, abstractmethod


class TestFilter(ABC):
    @abstractmethod
    def matches(self, test_name, result):
        pass


class SubstringFilter(TestFilter):
    def __init__(self, substring):
        self.substring = substring

    def matches(self, test_name, result):
        return self.substring in test_name


class RegexFilter(TestFilter):
    def __init__(self, pattern):
        self.pattern = re.compile(pattern)

    def matches(self, test_name, result):
        return bool(self.pattern.search(test_name))


class StateFilter(TestFilter):
    def __init__(self, state):
        self.state = state.lower()

    def matches(self, test_name, result):
        if not result:
            return False
        return result.pass_fail.lower() == self.state


class NotFilter(TestFilter):
    def __init__(self, sub_filter):
        self.sub_filter = sub_filter

    def matches(self, test_name, result):
        return not self.sub_filter.matches(test_name, result)


class CompositeFilter(TestFilter):
    def __init__(self):
        self.sub_filters = []

    def add_sub_filter(self, sub_filter):
        self.sub_filters.append(sub_filter)


class AndFilter(CompositeFilter):
    def matches(self, test_name, result):
        return all(
            sub_filter.matches(test_name, result) for sub_filter in self.sub_filters
        )


class OrFilter(CompositeFilter):
    def matches(self, test_name, result):
        if len(self.sub_filters) == 0:
            return True
        else:
            return any(
                sub_filter.matches(test_name, result) for sub_filter in self.sub_filters
            )
