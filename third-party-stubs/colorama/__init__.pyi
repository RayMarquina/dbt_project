from typing import Optional, Any

class Fore:
    RED: str = ...
    GREEN: str = ...
    YELLOW: str = ...

class Style:
    RESET_ALL: str = ...

def init(
    autoreset: bool = ...,
    convert: Optional[Any] = ...,
    strip: Optional[Any] = ...,
    wrap: bool = ...,
) -> None: ...
