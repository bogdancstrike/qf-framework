import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from colorama import Fore, Style, init, Back

# Initialize colorama for cross-platform support
init(autoreset=True, strip=False)

# Define a mapping for supported colors
COLOR_MAP = {
    "blue": Fore.BLUE,
    "blue_back": Back.BLUE,
    "red": Fore.RED,
    "red_back": Back.RED,
    "green": Fore.GREEN,
    "green_back": Back.GREEN,
    "yellow": Fore.YELLOW,
    "yellow_back": Back.YELLOW,
    "cyan": Fore.CYAN,
    "cyan_back": Back.CYAN,
    "magenta": Fore.MAGENTA,
    "magenta_back": Back.MAGENTA,
    "white": Fore.WHITE,
    "white_back": Back.WHITE,
    "gray": Fore.LIGHTBLACK_EX,
    "gray_back": Back.LIGHTBLACK_EX
}

# -----------------------------
# OpenTelemetry traceparent util
# -----------------------------
def _get_traceparent() -> str | None:
    """
    Returns W3C traceparent string: '00-<trace_id>-<span_id>-<flags>'
    or None if there is no valid current span context.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.trace.span import format_span_id, format_trace_id

        span = trace.get_current_span()
        ctx = span.get_span_context() if span is not None else None
        if ctx is None or not ctx.is_valid:
            return None

        version = "00"
        trace_id = format_trace_id(ctx.trace_id)  # 32 hex chars
        span_id = format_span_id(ctx.span_id)     # 16 hex chars
        flags = "01" if getattr(ctx.trace_flags, "sampled", False) else "00"

        return f"{version}-{trace_id}-{span_id}-{flags}"
    except Exception:
        # If opentelemetry isn't installed / misconfigured / etc.
        return None


class TraceParentFilter(logging.Filter):
    """
    Adds record.trace_parent as:
      - "[00-...]" when a traceparent exists
      - "[no-trace-parent]" otherwise
    """
    def filter(self, record: logging.LogRecord) -> bool:
        tp = _get_traceparent()
        record.trace_parent = f"[traceparent={tp}]" if tp else "[no-traceparent]"
        return True


# Custom Logger to handle color argument
class CustomLogger(logging.Logger):
    def _log(self, level, msg, args, exc_info=None, extra=None, stack_info=False, stacklevel=1):
        # Handle optional color argument (last positional argument)
        if isinstance(args, tuple) and len(args) > 0 and isinstance(args[-1], str) and args[-1].lower() in COLOR_MAP:
            color = args[-1].lower()
            msg = f"{COLOR_MAP[color]}{msg}{Style.RESET_ALL}"
            args = args[:-1]  # Remove the color argument from args
        super()._log(level, msg, args, exc_info, extra, stack_info, stacklevel)


# Set the custom logger as the default logger
logging.setLoggerClass(CustomLogger)

# Define the logger
logger = logging.getLogger("RotatingLog")
logger.setLevel(logging.DEBUG)  # Set minimum log level to DEBUG

# Add Stream Handler for console logging
console_handler = logging.StreamHandler()
console_handler.addFilter(TraceParentFilter())

# ✅ Use this (valid logging format key)
console_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s - %(trace_parent)s"
)
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)
