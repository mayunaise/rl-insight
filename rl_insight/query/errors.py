# Copyright (c) 2026 verl-project authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Query-router failures with a stable machine-readable reason (S2 §6.1).

Every rejection leaves the router as a non-success response carrying one of
these codes, so a client can tell "not found" (404) from "unsupported" (422)
from "the backend is down" (502) instead of reading an empty success.
"""

from __future__ import annotations

from typing import Any


class QueryError(RuntimeError):
    """A rejected run-scoped query, carrying its wire status and code."""

    def __init__(self, code: str, message: str = "", *, status: int = 400):
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message
        self.status = status

    def to_dict(self) -> dict[str, Any]:
        return {"reason_code": self.code, "message": self.message}
