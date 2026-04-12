import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import random
from server.async_controller import weighted_choice


def test_single_option():
    choices = [('only', 100)]
    assert weighted_choice(choices) == 'only'


def test_distribution_respects_weights():
    random.seed(42)
    choices = [('a', 90), ('b', 10)]
    results = [weighted_choice(choices) for _ in range(1000)]
    a_count = results.count('a')
    assert 800 < a_count < 980, f"Expected ~900 'a' results, got {a_count}"


def test_zero_weight_never_chosen():
    random.seed(42)
    choices = [('yes', 100), ('no', 0)]
    results = set(weighted_choice(choices) for _ in range(100))
    assert results == {'yes'}


def test_no_index_error_many_iterations():
    """Regression: old bisect-based implementation could IndexError."""
    random.seed(0)
    choices = [('a', 50), ('b', 50)]
    for _ in range(10000):
        result = weighted_choice(choices)
        assert result in ('a', 'b')
