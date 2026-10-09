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

"""TraceQL spanset enforcement and trace ownership (S2 §6.1/§6.3).

The shared Tempo holds every run's traces, so the exact Resource identity
attribute is forced onto every spanset of the query *before* it reaches the
backend; appending ``&& run_id`` to the end of the string would not constrain
the selectors that actually fetch data. The same identity is then re-checked on
the returned trace, because a trace ID is not globally unique and a trace may
mix spans from several runs.
"""

from __future__ import annotations

import re
from typing import Any

from .errors import QueryError

IDENTITY_SELECTOR_CONFLICT = "IDENTITY_SELECTOR_CONFLICT"
UNSUPPORTED_TRACEQL = "UNSUPPORTED_TRACEQL"
BACKEND_RESPONSE_INVALID = "BACKEND_RESPONSE_INVALID"

# The only pipeline stage the frozen dashboards need: a select projection.
_SELECT_STAGE = re.compile(r"^select\s*\([^()]*\)$", re.DOTALL)

_STRING_ESCAPES = {
    "a": "\a",
    "b": "\b",
    "f": "\f",
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "v": "\v",
    "\\": "\\",
    '"': '"',
}

_OCTAL_DIGITS = frozenset("01234567")
_ESCAPE_WIDTHS = {"x": 2, "u": 4, "U": 8}


def enforce_traceql(query: str, attribute: str, run_id: str) -> str:
    """Return ``query`` with the exact Resource identity on every spanset.

    An empty query becomes the identity-only spanset. Only the selector /
    logical-combination / ``select`` projection syntax is open; structural
    operators, ``||`` conditions and metric pipeline stages are rejected with
    ``UNSUPPORTED_TRACEQL`` instead of being passed on unconstrained.
    """
    text = query.strip()
    _reject_comments(text)
    if not text:
        return "{" + _identity(attribute, run_id) + "}"

    head, tail = _split_pipeline(text)
    blocks: list[str] = []
    for part in _split_spansets(head.strip()):
        block = part.strip()
        if not block.startswith("{"):
            raise _unsupported("only '{...}' spansets combined with '&&' are supported")
        end = _skip_braces(block, 0)
        if end != len(block):
            raise _unsupported("only '{...}' spansets combined with '&&' are supported")
        blocks.append("{" + _scope_conditions(block[1:-1], attribute, run_id) + "}")
    result = " && ".join(blocks)

    stage = tail.strip()
    if not stage:
        return result
    stage, rest = _split_pipeline(stage)
    stage = stage.strip()
    if rest.strip() or not _SELECT_STAGE.match(stage):
        raise _unsupported("only the 'select(...)' projection stage is supported")
    return f"{result} | {stage}"


def trace_ownership(payload: dict[str, Any], attribute: str, run_id: str) -> dict:
    """Check every Resource of a Tempo trace response against one run.

    ``owned`` means every Resource carries exactly this run's identity;
    ``mixed`` means the trace has spans of this run *and* of another one, which
    must never be forwarded as a half result. A response without batches (or
    without the attribute anywhere) is not owned.
    """
    if not isinstance(payload, dict):
        raise QueryError(
            BACKEND_RESPONSE_INVALID,
            "the trace response is not a JSON object",
            status=502,
        )
    batches = payload.get("batches") or []
    if not isinstance(batches, list):
        raise QueryError(
            BACKEND_RESPONSE_INVALID, "the trace response has no batch list", status=502
        )
    owned = False
    other = False
    for batch in batches:
        if _resource_identity(batch, attribute) == run_id:
            owned = True
        else:
            other = True
    return {"owned": owned and not other, "mixed": owned and other}


def _resource_identity(batch: Any, attribute: str) -> str | None:
    """The identity attribute of one batch's Resource, or ``None``."""
    if not isinstance(batch, dict) or not isinstance(batch.get("resource"), dict):
        raise QueryError(
            BACKEND_RESPONSE_INVALID, "a trace batch has no Resource", status=502
        )
    for item in batch["resource"].get("attributes") or []:
        if not isinstance(item, dict) or "key" not in item:
            raise QueryError(
                BACKEND_RESPONSE_INVALID,
                "a Resource attribute has no key",
                status=502,
            )
        if item["key"] == attribute:
            value = item.get("value")
            return value.get("stringValue") if isinstance(value, dict) else None
    return None


def _identity(attribute: str, run_id: str) -> str:
    escaped = run_id.replace("\\", "\\\\").replace('"', '\\"')
    return f'resource.{attribute} = "{escaped}"'


def _identity_condition(attribute: str) -> re.Pattern[str]:
    return re.compile(
        r"^resource\s*\.\s*"
        + re.escape(attribute)
        + r'\s*(=~|!~|!=|=)\s*("(?:[^"\\]|\\.)*"|`[^`]*`)$',
        re.DOTALL,
    )


def _scope_conditions(content: str, attribute: str, run_id: str) -> str:
    """Merge the identity condition into one spanset's condition list."""
    if len(_split_top_level(content, "||")) > 1:
        raise _unsupported("'||' inside one spanset cannot be scoped to a run")
    parts = [part.strip() for part in _split_top_level(content, "&&") if part.strip()]
    present = False
    pattern = _identity_condition(attribute)
    for part in parts:
        match = pattern.match(part)
        if match is None:
            continue
        if match.group(1) != "=" or _unquote(match.group(2)) != run_id:
            raise QueryError(
                IDENTITY_SELECTOR_CONFLICT,
                f"the identity attribute {attribute!r} is already constrained elsewhere",
                status=422,
            )
        present = True
    if not present:
        parts.append(_identity(attribute, run_id))
    return " && ".join(parts)


def _unsupported(message: str) -> QueryError:
    return QueryError(UNSUPPORTED_TRACEQL, message, status=422)


def _split_pipeline(text: str) -> tuple[str, str]:
    """Split at the first ``|`` outside strings and outside a spanset."""
    depth = 0
    index = 0
    while index < len(text):
        char = text[index]
        if char in '"`':
            index = _skip_string(text, index)
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
        elif char == "|" and depth == 0:
            return text[:index], text[index + 1 :]
        index += 1
    return text, ""


def _split_spansets(text: str) -> list[str]:
    """Split a first stage on the ``&&`` that combines spansets.

    ``&&`` inside ``{...}`` combines conditions of one spanset; only a ``&&``
    between two closing and opening braces separates selectors that each fetch
    data.
    """
    parts: list[str] = []
    buffer: list[str] = []
    depth = 0
    index = 0
    while index < len(text):
        char = text[index]
        if char in '"`':
            end = _skip_string(text, index)
            buffer.append(text[index:end])
            index = end
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
        if depth == 0 and text.startswith("&&", index):
            parts.append("".join(buffer))
            buffer = []
            index += 2
            continue
        buffer.append(char)
        index += 1
    parts.append("".join(buffer))
    return parts


def _split_top_level(text: str, separator: str) -> list[str]:
    """Split on ``separator`` outside string literals."""
    parts: list[str] = []
    buffer: list[str] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char in '"`':
            end = _skip_string(text, index)
            buffer.append(text[index:end])
            index = end
        elif text.startswith(separator, index):
            parts.append("".join(buffer))
            buffer = []
            index += len(separator)
        else:
            buffer.append(char)
            index += 1
    parts.append("".join(buffer))
    return parts


def _skip_string(text: str, index: int) -> int:
    """Return the index just past the string literal starting at ``index``."""
    if text[index] == "`":
        end = text.find("`", index + 1)
        if end < 0:
            raise _unsupported("unterminated string literal")
        return end + 1
    position = index + 1
    while position < len(text):
        if text[position] == "\\":
            position += 2
        elif text[position] == '"':
            return position + 1
        else:
            position += 1
    raise _unsupported("unterminated string literal")


def _skip_braces(text: str, index: int) -> int:
    """Return the index just past the ``{...}`` spanset starting at ``index``."""
    depth = 0
    position = index
    while position < len(text):
        char = text[position]
        if char in '"`':
            position = _skip_string(text, position)
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return position + 1
        position += 1
    raise _unsupported("unbalanced braces in the query")


def _reject_comments(text: str) -> None:
    """Refuse TraceQL comments instead of scanning around them.

    Tempo lexes TraceQL with Go's ``text/scanner`` in its default mode, which
    skips ``//`` and ``/* */`` comments. This module's scanners do not, so a
    comment could hide a ``||`` or swallow an injected identity condition -
    and a query whose identity cannot be proven is refused (S2 §6.1). The
    frozen dashboards do not use comments, so nothing legitimate is lost.
    """
    index = 0
    while index < len(text):
        char = text[index]
        if char in '"`':
            index = _skip_string(text, index)
            continue
        if text.startswith("//", index) or text.startswith("/*", index):
            raise _unsupported(
                "comments are not supported in a run-scoped TraceQL query"
            )
        index += 1


def _unquote(value: str) -> str:
    """The string a TraceQL string literal holds, for exact comparison.

    The decode covers Go's full escape set (octal, ``\\x``, ``\\u``, ``\\U``),
    because that is what the TraceQL lexer applies. An incomplete decode would
    let the identity condition name another run's value while looking equal.
    """
    if value.startswith("`"):
        return value[1:-1]
    body = value[1:-1]
    out: list[str] = []
    index = 0
    while index < len(body):
        char = body[index]
        if char != "\\":
            out.append(char)
            index += 1
            continue
        escaped = body[index + 1 : index + 2]
        if escaped in _OCTAL_DIGITS:
            out.append(_decode_escape(body, index + 1, 3, 8))
            index += 4
        elif escaped in _ESCAPE_WIDTHS:
            width = _ESCAPE_WIDTHS[escaped]
            out.append(_decode_escape(body, index + 2, width, 16))
            index += 2 + width
        elif escaped in _STRING_ESCAPES:
            out.append(_STRING_ESCAPES[escaped])
            index += 2
        else:
            raise _unsupported(f"unsupported string escape \\{escaped}")
    return "".join(out)


def _decode_escape(body: str, start: int, width: int, base: int) -> str:
    digits = body[start : start + width]
    try:
        if len(digits) != width:
            raise ValueError("truncated escape")
        return chr(int(digits, base))
    except ValueError as exc:
        raise _unsupported(f"invalid string escape \\{digits}") from exc
