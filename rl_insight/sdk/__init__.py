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

"""Trainer-side RL-Insight SDK: run identity, context, config and publisher.

The public entry points live in :mod:`rl_insight.sdk.session`
(``open_session`` / ``attach``); the remaining modules hold the protocol
types they need. Nothing here is imported by ``rl_insight`` eagerly.
"""

from .metrics import IdentityMetricsServer, MetricsEndpoint

__all__ = ["IdentityMetricsServer", "MetricsEndpoint"]
