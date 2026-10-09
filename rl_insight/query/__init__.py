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

"""Run-scoped query planning, identity enforcement and the query router.

S1 §7 plans where raw samples come from; S2 §6 forces the run identity onto
every query that reaches the shared Prometheus/Tempo.
"""

from __future__ import annotations

from .errors import QueryError
from .planner import (
    ARCHIVE,
    ARCHIVE_QUERY_UNAVAILABLE,
    CROSS,
    CROSS_BOUNDARY_QUERY_UNSUPPORTED,
    NONE,
    ONLINE,
    OUT_OF_COVERAGE,
    RAW_SAMPLE_CONFLICT,
    Coverage,
    MergeResult,
    QueryPlan,
    merge_raw_samples,
    plan_query,
)
from .promql import enforce_match_selector, enforce_selector, identity_selector
from .router import create_query_router
from .tempo import enforce_traceql, trace_ownership

__all__ = [
    "ARCHIVE",
    "ARCHIVE_QUERY_UNAVAILABLE",
    "CROSS",
    "CROSS_BOUNDARY_QUERY_UNSUPPORTED",
    "NONE",
    "ONLINE",
    "OUT_OF_COVERAGE",
    "RAW_SAMPLE_CONFLICT",
    "Coverage",
    "MergeResult",
    "QueryError",
    "QueryPlan",
    "create_query_router",
    "enforce_match_selector",
    "enforce_selector",
    "enforce_traceql",
    "identity_selector",
    "merge_raw_samples",
    "plan_query",
    "trace_ownership",
]
