import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'examples' / 'alerts' / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Context:
    def __init__(self, params):
        self.params = params
        self.exchange = 'bulkf'
        self.timestamp_ms = 0
        self.alerts = []

    def alert(self, value):
        self.alerts.append(value)
        return {'status': 'simulated'}


class History:
    def __init__(self):
        self.values = {}

    def source(self, selector, offset=0):
        return self.values.get(selector)


class AlertExamples(unittest.TestCase):
    def event(self, module, ctx, history, exchange, timestamp, price):
        ctx.exchange, ctx.timestamp_ms = exchange, timestamp
        history.values[f'btc@trades@{exchange}'] = {'price': price, 'size': 1}
        return module.on_data(ctx, history)

    def test_smoke_only_sends_once(self):
        module, ctx, history = load('telegram-smoke'), Context({}), History()
        for timestamp in (1000, 2000, 3000):
            self.event(module, ctx, history, 'bulkf', timestamp, 83000)
        self.assertEqual(len(ctx.alerts), 1)

    def test_price_requires_crossing_not_initial_above(self):
        module = load('price-crossing')
        ctx, history = Context({'price': 100, 'cooldown_seconds': 60}), History()
        for timestamp, price in enumerate((101, 102, 99, 100, 101), 1):
            self.event(module, ctx, history, 'bulkf', timestamp, price)
        self.assertEqual(len(ctx.alerts), 1)

    def sequence(self):
        return load('multi-exchange-sequence'), Context({'breakout': 100, 'spread_bps': 10, 'window_seconds': 30, 'fresh_seconds': 5, 'cooldown_seconds': 300}), History()

    def test_sequence_requires_later_fresh_confirmation_and_cooldown(self):
        module, ctx, history = self.sequence()
        for exchange, timestamp, price in [('hyperliquidf', 1000, 101), ('bulkf', 2000, 99), ('bulkf', 3000, 100), ('hyperliquidf', 4000, 101), ('bulkf', 5000, 99), ('bulkf', 6000, 100), ('hyperliquidf', 7000, 101)]:
            self.event(module, ctx, history, exchange, timestamp, price)
        self.assertEqual(len(ctx.alerts), 1)

    def test_sequence_rejects_expired_stale_and_out_of_order_data(self):
        for confirmation in (9000, 40000):
            module, ctx, history = self.sequence()
            self.event(module, ctx, history, 'bulkf', 1000, 99)
            self.event(module, ctx, history, 'bulkf', 2000, 100)
            self.event(module, ctx, history, 'hyperliquidf', confirmation, 101)
            self.assertEqual(ctx.alerts, [])
        module, ctx, history = self.sequence()
        self.event(module, ctx, history, 'bulkf', 3000, 99)
        self.event(module, ctx, history, 'bulkf', 2000, 100)
        self.event(module, ctx, history, 'hyperliquidf', 4000, 101)
        self.assertEqual(ctx.alerts, [])


if __name__ == '__main__':
    unittest.main()
