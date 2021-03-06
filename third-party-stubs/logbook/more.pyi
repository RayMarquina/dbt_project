# Stubs for logbook.more (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.

from logbook.base import RecordDispatcher
from logbook.handlers import (
    FingersCrossedHandler as FingersCrossedHandlerBase,
    Handler,
    StderrHandler,
    StringFormatter,
    StringFormatterHandlerMixin,
)
from logbook.ticketing import BackendBase
from typing import Any, Optional

TWITTER_FORMAT_STRING: Any
TWITTER_ACCESS_TOKEN_URL: str
NEW_TWEET_URL: str

class CouchDBBackend(BackendBase):
    database: Any = ...
    def setup_backend(self) -> None: ...
    def record_ticket(self, record: Any, data: Any, hash: Any, app_id: Any) -> None: ...

class TwitterFormatter(StringFormatter):
    max_length: int = ...
    def format_exception(self, record: Any): ...
    def __call__(self, record: Any, handler: Any): ...

class TaggingLogger(RecordDispatcher):
    def __init__(self, name: Optional[Any] = ..., tags: Optional[Any] = ...) -> None: ...
    def log(self, tags: Any, msg: Any, *args: Any, **kwargs: Any): ...

class TaggingHandler(Handler):
    def __init__(self, handlers: Any, filter: Optional[Any] = ..., bubble: bool = ...) -> None: ...
    def emit(self, record: Any) -> None: ...

class TwitterHandler(Handler, StringFormatterHandlerMixin):
    default_format_string: Any = ...
    formatter_class: Any = ...
    consumer_key: Any = ...
    consumer_secret: Any = ...
    username: Any = ...
    password: Any = ...
    def __init__(
        self,
        consumer_key: Any,
        consumer_secret: Any,
        username: Any,
        password: Any,
        level: Any = ...,
        format_string: Optional[Any] = ...,
        filter: Optional[Any] = ...,
        bubble: bool = ...,
    ) -> None: ...
    def get_oauth_token(self): ...
    def make_client(self): ...
    def tweet(self, status: Any): ...
    def emit(self, record: Any) -> None: ...

class SlackHandler(Handler, StringFormatterHandlerMixin):
    api_token: Any = ...
    channel: Any = ...
    slack: Any = ...
    def __init__(
        self,
        api_token: Any,
        channel: Any,
        level: Any = ...,
        format_string: Optional[Any] = ...,
        filter: Optional[Any] = ...,
        bubble: bool = ...,
    ) -> None: ...
    def emit(self, record: Any) -> None: ...

class JinjaFormatter:
    template: Any = ...
    def __init__(self, template: Any) -> None: ...
    def __call__(self, record: Any, handler: Any): ...

class ExternalApplicationHandler(Handler):
    encoding: Any = ...
    def __init__(
        self,
        arguments: Any,
        stdin_format: Optional[Any] = ...,
        encoding: str = ...,
        level: Any = ...,
        filter: Optional[Any] = ...,
        bubble: bool = ...,
    ) -> None: ...
    def emit(self, record: Any) -> None: ...

class ColorizingStreamHandlerMixin:
    def force_color(self) -> None: ...
    def forbid_color(self) -> None: ...
    def should_colorize(self, record: Any): ...
    def get_color(self, record: Any): ...
    def format(self, record: Any): ...

class ColorizedStderrHandler(ColorizingStreamHandlerMixin, StderrHandler):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

class FingersCrossedHandler(FingersCrossedHandlerBase):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

class ExceptionHandler(Handler, StringFormatterHandlerMixin):
    exc_type: Any = ...
    def __init__(
        self,
        exc_type: Any,
        level: Any = ...,
        format_string: Optional[Any] = ...,
        filter: Optional[Any] = ...,
        bubble: bool = ...,
    ) -> None: ...
    def handle(self, record: Any): ...

class DedupHandler(Handler):
    def __init__(self, format_string: str = ..., *args: Any, **kwargs: Any) -> None: ...
    def clear(self) -> None: ...
    def pop_application(self) -> None: ...
    def pop_thread(self) -> None: ...
    def pop_context(self) -> None: ...
    def pop_greenlet(self) -> None: ...
    def handle(self, record: Any): ...
    def flush(self) -> None: ...

class RiemannHandler(Handler):
    host: Any = ...
    port: Any = ...
    ttl: Any = ...
    queue: Any = ...
    flush_threshold: Any = ...
    transport: Any = ...
    def __init__(
        self,
        host: Any,
        port: Any,
        message_type: str = ...,
        ttl: int = ...,
        flush_threshold: int = ...,
        bubble: bool = ...,
        filter: Optional[Any] = ...,
        level: Any = ...,
    ) -> None: ...
    def record_to_event(self, record: Any): ...
    def emit(self, record: Any) -> None: ...
