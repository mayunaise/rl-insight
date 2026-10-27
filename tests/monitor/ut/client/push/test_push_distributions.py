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

import pytest
from omegaconf import OmegaConf
from rl_insight.client.push.translate import ScrapeState


def histogram(buckets, total, count, replica="0"):
    text = "# TYPE latency histogram\n"
    for bound, value in zip(["1", "2", "+Inf"], buckets):
        text += f'latency_bucket{{replica="{replica}",le="{bound}"}} {value}\n'
    return (
        text
        + f'latency_sum{{replica="{replica}"}} {total}\nlatency_count{{replica="{replica}"}} {count}\n'
    )


def rules():
    return OmegaConf.create(
        [
            {
                "type": "histogram",
                "source": "latency",
                "name": "latency",
                "scale": 1000,
                "presence": "source_available",
            }
        ]
    )


def test_quantiles_use_bucket_deltas_and_preserve_full_distribution():
    state = ScrapeState()
    baseline = histogram([10, 20, 20], 25, 20)
    assert [e.name for e in state.translate(baseline, rules())] == ["source_available"]
    out = state.translate(histogram([12, 24, 24], 30, 24), rules())
    quantiles = {
        e.tags["quantile"]: e.value for e in out if e.name.endswith(".quantile")
    }
    assert quantiles == {"0.5": 1000, "0.9": 1800, "0.95": 1900, "0.99": 1980}
    assert next(e.value for e in out if e.name.endswith(".mean")) == 1250
    assert [e.value for e in out if e.name.endswith(".bucket")] == [2, 4, 4]
    assert next(e.value for e in out if e.name.endswith(".count")) == 4
    # A second idle poll preserves zero counter increments but has no quantiles.
    assert not any(
        e.name.endswith(".quantile")
        for e in state.translate(histogram([12, 24, 24], 30, 24), rules())
    )


def test_histogram_reset_rebaselines_without_spikes():
    state = ScrapeState()
    state.translate(histogram([10, 20, 20], 25, 20), rules())
    out = state.translate(histogram([1, 2, 2], 3, 2), rules())
    assert [e.name for e in out] == ["source_available"]
    out = state.translate(histogram([2, 4, 4], 6, 4), rules())
    assert next(e.value for e in out if e.name.endswith(".mean")) == 1500


def test_histogram_series_are_isolated_and_missing_is_not_zero_latency():
    state = ScrapeState()
    assert state.translate("", rules())[0].value == 0
    state.translate(
        histogram([0, 0, 0], 0, 0) + histogram([0, 0, 0], 0, 0, "1"), rules()
    )
    out = state.translate(
        histogram([1, 1, 1], 0.5, 1) + histogram([0, 1, 1], 1.5, 1, "1"), rules()
    )
    means = {e.tags["replica"]: e.value for e in out if e.name.endswith(".mean")}
    assert means == {"0": 500, "1": 1500}


@pytest.mark.parametrize(
    "buckets,count", [([2, 1, 3], 3), ([-1, 1, 3], 3), ([1, 2, 3], 4)]
)
def test_inconsistent_histogram_is_not_reported_as_a_valid_quantile(buckets, count):
    state = ScrapeState()
    state.translate(histogram([0, 0, 0], 0, 0), rules())
    out = state.translate(histogram(buckets, 3, count), rules())
    assert not any(e.name.endswith(".quantile") for e in out)


def test_counter_ratios_join_labels_and_reset():
    state = ScrapeState()
    rule = OmegaConf.create(
        [
            {
                "type": "counter_ratio",
                "numerator": "hits_total",
                "denominator": "queries_total",
                "name": "hit_rate",
            }
        ]
    )

    def scrape(a, b):
        return f'# TYPE hits_total counter\nhits_total{{engine="0"}} {a}\n# TYPE queries_total counter\nqueries_total{{engine="0"}} {b}\n'

    assert state.translate(scrape(10, 100), rule) == []
    assert state.translate(scrape(12, 104), rule)[0].value == 0.5
    assert state.translate(scrape(1, 2), rule) == []
    assert state.translate(scrape(2, 4), rule)[0].value == 0.5


def test_histogram_sum_can_feed_a_counter_delta_rule():
    state = ScrapeState()
    rule = OmegaConf.create(
        [{"type": "counter_delta", "source": "latency_sum", "name": "total_latency"}]
    )
    assert state.translate(histogram([1, 2, 2], 3, 2), rule) == []
    assert state.translate(histogram([2, 4, 4], 7, 4), rule)[0].value == 4
