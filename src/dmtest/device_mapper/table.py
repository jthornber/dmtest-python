from typing import List, Iterator, Tuple
from .targets import Target  # Assuming Target is defined in a 'targets' module

class Table:
    def __init__(self, *targets: Target) -> None:
        self._targets: Tuple[Target, ...] = targets

    def __len__(self) -> int:
        return len(self._targets)

    def __iter__(self) -> Iterator[Target]:
        return iter(self._targets)

    def table_lines(self) -> str:
        start_sector: int = 0
        lines: List[str] = []
        for t in self._targets:
            args: str = " ".join(map(str, t.args))
            line: str = f"{start_sector} {t.sector_count} {t.type} {args}"
            lines.append(line)
            start_sector += t.sector_count
        return "\n".join(lines)
