import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.ip_utils import undot_ipv4, dot_ipv4, range_ipv4


def test_undot_ipv4():
    assert undot_ipv4('0.0.0.0') == 0
    assert undot_ipv4('255.255.255.255') == 0xFFFFFFFF
    assert undot_ipv4('10.0.0.1') == (10 << 24) + 1


def test_dot_ipv4():
    assert dot_ipv4(0) == '0.0.0.0'
    assert dot_ipv4(0xFFFFFFFF) == '255.255.255.255'
    assert dot_ipv4((192 << 24) + (168 << 16) + 1) == '192.168.0.1'


def test_range_ipv4_inclusive():
    result = list(range_ipv4('10.0.0.1', '10.0.0.3'))
    assert result == ['10.0.0.1', '10.0.0.2', '10.0.0.3']


def test_range_ipv4_single():
    result = list(range_ipv4('10.0.0.5', '10.0.0.5'))
    assert result == ['10.0.0.5']


def test_range_ipv4_across_octet():
    result = list(range_ipv4('10.0.0.254', '10.0.1.1'))
    assert result == ['10.0.0.254', '10.0.0.255', '10.0.1.0', '10.0.1.1']
