class Table:
    def __init__(self, *targets):
        self._targets = targets

    def __len__(self):
        len(self._targets)

    def __iter__(self):
        return iter(self._targets)

    def table_lines(self):
        start_sector = 0
        lines = []
        for t in self._targets:
            args = " ".join(map(str, t.args))
            line = f"{start_sector} {t.sector_count} {t.type} {args}"
            lines.append(line)
            start_sector += t.sector_count
        return "\n".join(lines)
