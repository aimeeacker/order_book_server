from __future__ import annotations

import warnings

from hl_book import *  # noqa: F401,F403

warnings.warn(
    "hl_py is deprecated; use `import hl_book` instead.",
    DeprecationWarning,
    stacklevel=2,
)
