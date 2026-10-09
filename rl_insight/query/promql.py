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

"""Run-scoped PromQL selector enforcement (S2 §6.1/§6.2).

The shared Prometheus also holds every other run, so the exact identity label
must be forced onto *every* vector selector before the expression reaches the
backend. Nothing is evaluated here: a small tokenizer finds selector contexts,
merges the identity matcher into them and copies every other byte unchanged, so
ranges, offsets, ``@`` modifiers and subqueries keep the backend's own
semantics. Filtering the backend result afterwards is not isolation and is
never done.
"""

from __future__ import annotations

import re
from string import ascii_letters, digits

from .errors import QueryError

IDENTITY_SELECTOR_CONFLICT = "IDENTITY_SELECTOR_CONFLICT"
UNSUPPORTED_EXPRESSION = "UNSUPPORTED_EXPRESSION"

_IDENT_START = frozenset(ascii_letters) | {"_"}
_IDENT_CHARS = frozenset(ascii_letters) | frozenset(digits) | {"_", ":"}
_DIGITS = frozenset(digits)
_BLANK = " \t\r\n"

# The frozen backend is Prometheus 2.54.1 (``server.install_version``). Its
# grammar decides which words may name a metric: ``metric_identifier`` accepts
# the aggregation operators (``sum``, ``count``, ``topk``, ...), the logical
# operators (``and``, ``or``, ``unless``), ``by``/``without``, ``offset`` and
# ``start``/``end`` — so a metric really can be called ``sum`` or ``start``, and
# skipping those words would leave their selector without an identity matcher.
# This set is the complement: the tokens that grammar refuses as a metric name
# (``bool``/``on``/``ignoring``/``group_left``/``group_right``/``atan2`` are
# keywords only, and ``inf``/``nan`` are number literals). Copying those through
# cannot leak another run's series.
_NEVER_METRIC = frozenset(
    {"bool", "on", "ignoring", "group_left", "group_right", "atan2", "inf", "nan"}
)

# Aggregation operators: only an ``(`` or a grouping keyword after the word
# makes it an aggregation; on their own they are metric names.
_AGGREGATORS = frozenset(
    {
        "sum",
        "avg",
        "min",
        "max",
        "count",
        "stddev",
        "stdvar",
        "group",
        "topk",
        "bottomk",
        "count_values",
        "quantile",
        "limitk",
        "limit_ratio",
    }
)
_GROUPING = frozenset({"by", "without"})

# Keywords whose parenthesised argument is a label list, not an expression.
_LABEL_LISTS = frozenset(
    {"by", "without", "on", "ignoring", "group_left", "group_right"}
)

# Words that are both a metric name and an infix operator or modifier. Only the
# grammar position decides: after a complete operand they are the operator, at
# the start of an operand they are the metric. Getting this wrong in the other
# direction would inject an identity matcher into an operator and turn a valid
# query into a backend 400, which is why position, not the word alone, decides.
_AMBIGUOUS = frozenset({"and", "or", "unless", "offset"})

# Punctuation that starts an operand; everything else outside blanks and the
# closing brackets is an operator character.
_OPERAND_START = frozenset("([,")
_OPERAND_END = frozenset(")]}")

# A number or duration (``0.5``, ``1e-3``, ``100ms``, ``1h30m``): its unit
# letters must never be mistaken for a metric name.
_NUMBER = re.compile(
    r"(?:\d+\.?\d*(?:[eE][+-]?\d+)?|\.\d+)(?:ms|[smhdwy])?(?:\d+(?:ms|[smhdwy]))*"
)

_MATCHER = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(=~|!~|!=|=)\s*"
    r'("(?:[^"\\]|\\.)*"|`[^`]*`)\s*$',
    re.DOTALL,
)

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


def enforce_selector(expression: str, run_id: str, label: str) -> str:
    """Return ``expression`` with an exact ``label=run_id`` matcher on every selector.

    An existing matcher on ``label`` is kept only when it already pins exactly
    this value; a regex, negation or different value is rejected instead of
    being quietly overwritten.
    """
    rewritten, _ = _rewrite(expression, run_id, label)
    return rewritten


def enforce_match_selector(match: str, run_id: str, label: str) -> str:
    """Return one run-scoped ``match[]`` series selector.

    A ``match[]`` entry must be a single series selector: anything else (an
    expression, a function call, two selectors) cannot be proven run-scoped and
    is rejected.
    """
    text = match.strip()
    if not text:
        raise QueryError(
            UNSUPPORTED_EXPRESSION,
            "an empty match[] is not a series selector",
            status=422,
        )
    rewritten, spans = _rewrite(text, run_id, label)
    if len(spans) != 1 or text[spans[0][0] : spans[0][1]].strip() != text:
        raise QueryError(
            UNSUPPORTED_EXPRESSION,
            "a match[] entry must be a single series selector",
            status=422,
        )
    return rewritten


def identity_selector(run_id: str, label: str) -> str:
    """The exact identity selector ``{label="run_id"}``, escaped for PromQL."""
    return "{" + _matcher(label, run_id) + "}"


def _matcher(label: str, run_id: str) -> str:
    escaped = run_id.replace("\\", "\\\\").replace('"', '\\"')
    return f'{label}="{escaped}"'


def _rewrite(
    expression: str, run_id: str, label: str
) -> tuple[str, list[tuple[int, int]]]:
    """Rewrite every selector of ``expression`` and report their source spans."""
    out: list[str] = []
    spans: list[tuple[int, int]] = []
    index, size = 0, len(expression)
    # Whether the next significant token starts an operand (a metric name, a
    # number, a parenthesised expression). It is what tells an operator word
    # such as ``and`` from an identically named metric.
    operand = True
    while index < size:
        char = expression[index]
        if char in '"`':
            end = _skip_string(expression, index)
            out.append(expression[index:end])
            index = end
            operand = False
        elif char == "#":
            # PromQL's line comment: its text is copied verbatim and never
            # scanned for selectors, so a quote inside a comment cannot hide a
            # real selector from the identity injection.
            end = _skip_comment(expression, index)
            out.append(expression[index:end])
            index = end
        elif char == "{":
            end = _skip_braces(expression, index)
            out.append(
                "{"
                + _scope_matchers(expression[index + 1 : end - 1], run_id, label)
                + "}"
            )
            spans.append((index, end))
            index = end
            operand = False
        elif char in _IDENT_START:
            end = index + 1
            while end < size and expression[end] in _IDENT_CHARS:
                end += 1
            word = expression[index:end]
            after = _skip_blanks(expression, end)
            following = expression[after] if after < size else ""
            if word in _LABEL_LISTS and following == "(":
                close = _skip_parens(expression, after)
                out.append(expression[index:close])
                index = close
                operand = False
            elif following == "(":
                # A function call or an aggregation: never a vector selector,
                # and its argument is scanned on its own.
                out.append(word)
                index = end
                operand = False
            elif word in _AGGREGATORS and _following_word(expression, end) in _GROUPING:
                # ``sum by (x) (...)``: the word is the aggregation. The
                # grouping keyword that follows is copied by its own branch.
                out.append(word)
                index = end
                operand = False
            elif word in _NEVER_METRIC:
                # No metric can carry this name, so it is never a selector; the
                # operand it introduces (``a == bool b``) is still to come.
                out.append(word)
                index = end
            elif word in _AMBIGUOUS and not operand:
                out.append(word)
                index = end
                operand = True
            elif following == "{":
                close = _skip_braces(expression, after)
                out.append(
                    word
                    + "{"
                    + _scope_matchers(expression[after + 1 : close - 1], run_id, label)
                    + "}"
                )
                spans.append((index, close))
                index = close
                operand = False
            else:
                out.append(word + identity_selector(run_id, label))
                spans.append((index, end))
                index = end
                operand = False
        elif char == "$":
            # A Grafana variable (``$project``, ``$__rate_interval``, ``${x}``)
            # is substituted by Grafana, never a metric name.
            end = index + 1
            if end < size and expression[end] == "{":
                end = _skip_braces(expression, end)
            else:
                while end < size and expression[end] in _IDENT_CHARS:
                    end += 1
            out.append(expression[index:end])
            index = end
            operand = False
        elif char in _DIGITS:
            number = _NUMBER.match(expression, index)
            assert number is not None  # a digit always starts a number
            out.append(expression[index : number.end()])
            index = number.end()
            operand = False
        else:
            out.append(char)
            if char in _OPERAND_START:
                operand = True
            elif char in _OPERAND_END:
                operand = False
            elif char not in _BLANK:
                # An operator (``+``, ``==``, ``@``, ``:``) introduces an
                # operand; a failed guess can only make a leaking selector an
                # error, never the other way round.
                operand = True
            index += 1
    return "".join(out), spans


def _skip_blanks(text: str, index: int) -> int:
    """Return the index of the first non-blank character at or after ``index``."""
    size = len(text)
    while index < size and text[index] in _BLANK:
        index += 1
    return index


def _following_word(text: str, index: int) -> str:
    """The next identifier word after blanks and comments, or ``""``."""
    size = len(text)
    position = index
    while position < size:
        if text[position] in _BLANK:
            position += 1
        elif text[position] == "#":
            position = _skip_comment(text, position)
        else:
            break
    if position >= size or text[position] not in _IDENT_START:
        return ""
    end = position + 1
    while end < size and text[end] in _IDENT_CHARS:
        end += 1
    return text[position:end]


def _scope_matchers(content: str, run_id: str, label: str) -> str:
    """Merge the identity matcher into one ``{...}`` matcher set."""
    matchers: list[str] = []
    present = False
    for part in _split_top_level(content, ","):
        if not part.strip():
            continue
        match = _MATCHER.match(part)
        if match is None:
            raise QueryError(
                UNSUPPORTED_EXPRESSION,
                f"cannot scope the selector {{{content.strip()}}}",
                status=422,
            )
        name, operator, value = match.group(1), match.group(2), match.group(3)
        if name == label:
            if operator != "=" or _unquote(value) != run_id:
                raise QueryError(
                    IDENTITY_SELECTOR_CONFLICT,
                    f"the identity label {label!r} is already constrained elsewhere",
                    status=422,
                )
            present = True
        matchers.append(part.strip())
    if not present:
        matchers.append(_matcher(label, run_id))
    return ", ".join(matchers)


def _split_top_level(text: str, separator: str) -> list[str]:
    """Split on ``separator`` outside string literals and comments."""
    parts: list[str] = []
    buffer: list[str] = []
    index, size = 0, len(text)
    while index < size:
        char = text[index]
        if char in '"`':
            end = _skip_string(text, index)
            buffer.append(text[index:end])
            index = end
        elif char == "#":
            index = _skip_comment(text, index)
        elif text.startswith(separator, index):
            parts.append("".join(buffer))
            buffer = []
            index += len(separator)
        else:
            buffer.append(char)
            index += 1
    parts.append("".join(buffer))
    return parts


def _skip_comment(text: str, index: int) -> int:
    """Return the index of the newline ending the ``#`` comment, or the end."""
    end = text.find("\n", index)
    return len(text) if end < 0 else end


def _skip_string(text: str, index: int) -> int:
    """Return the index just past the string literal starting at ``index``."""
    if text[index] == "`":
        end = text.find("`", index + 1)
        if end < 0:
            raise QueryError(
                UNSUPPORTED_EXPRESSION, "unterminated string literal", status=422
            )
        return end + 1
    position = index + 1
    while position < len(text):
        if text[position] == "\\":
            position += 2
        elif text[position] == '"':
            return position + 1
        else:
            position += 1
    raise QueryError(UNSUPPORTED_EXPRESSION, "unterminated string literal", status=422)


def _skip_braces(text: str, index: int) -> int:
    """Return the index just past the ``{...}`` set starting at ``index``."""
    depth = 0
    position = index
    while position < len(text):
        char = text[position]
        if char == "#":
            position = _skip_comment(text, position)
            continue
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
    raise QueryError(
        UNSUPPORTED_EXPRESSION, "unbalanced braces in the expression", status=422
    )


def _skip_parens(text: str, index: int) -> int:
    """Return the index just past the ``(...)`` group starting at ``index``."""
    depth = 0
    position = index
    while position < len(text):
        char = text[position]
        if char == "#":
            position = _skip_comment(text, position)
            continue
        if char in '"`':
            position = _skip_string(text, position)
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return position + 1
        position += 1
    raise QueryError(
        UNSUPPORTED_EXPRESSION, "unbalanced parentheses in the expression", status=422
    )


def _unquote(value: str) -> str:
    """The string a PromQL string literal holds, for exact comparison.

    The decode covers Go's full escape set (octal, ``\\x``, ``\\u``, ``\\U``),
    because that is what the Prometheus parser applies. An incomplete decode
    would let ``run_id="x\\101"`` pass as *another* run's ID, and an unknown
    escape is rejected rather than compared against an assumed value.
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
            raise QueryError(
                UNSUPPORTED_EXPRESSION,
                f"unsupported string escape \\{escaped} in the expression",
                status=422,
            )
    return "".join(out)


def _decode_escape(body: str, start: int, width: int, base: int) -> str:
    digits = body[start : start + width]
    try:
        if len(digits) != width:
            raise ValueError("truncated escape")
        return chr(int(digits, base))
    except ValueError as exc:
        raise QueryError(
            UNSUPPORTED_EXPRESSION,
            f"invalid string escape \\{digits} in the expression",
            status=422,
        ) from exc
