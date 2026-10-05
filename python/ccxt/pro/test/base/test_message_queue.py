import asyncio
import os
import sys

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
sys.path.append(root)

import ccxt.pro as ccxtpro
from ccxt import NetworkError
from ccxt.async_support.base.ws.client import Client


def create_client(capacity=10, hashes=None):
    def callback(*_):
        return None

    return Client(
        'wss://example.invalid',
        callback,
        callback,
        callback,
        callback,
        {
            'messageQueueHashes': hashes or ['positions'],
            'messageQueueCapacity': capacity,
        },
    )


async def test_message_queue():
    client = create_client()
    first = client.future('positions')
    first_payload = [{'id': 'first'}]
    client.resolve(first_payload, 'positions')
    client.resolve([{'id': 'second'}], 'positions')
    first_payload[0]['id'] = 'mutated'

    assert await first == [{'id': 'first'}]
    assert await client.future('positions') == [{'id': 'second'}]

    unconfigured = client.future('orders')
    client.resolve('first', 'orders')
    client.resolve('discarded', 'orders')
    assert await unconfigured == 'first'
    assert not client.future('orders').done()
    assert 'orders' not in client.messageQueue

    overflow_client = create_client(capacity=1)
    overflow_client.resolve([{'id': 'first'}], 'positions')
    overflow_client.resolve([{'id': 'second'}], 'positions')
    try:
        await overflow_client.future('positions')
        assert False, 'Expected queue overflow'
    except NetworkError as error:
        assert 'queue overflow' in str(error)
    assert overflow_client.messageQueueSize == 0

    rejected_client = create_client()
    rejected_client.resolve([{'id': 'stale'}], 'positions')
    rejected_client.reject(NetworkError('disconnected'))
    assert rejected_client.messageQueueSize == 0


async def test_private_stream_batches():
    bybit = ccxtpro.bybit({'newUpdates': True})
    bybit.newUpdates = True
    bybit_client = create_client(hashes=['orders', 'myTrades', 'positions'])
    bybit.parse_order = lambda order: dict(order)
    bybit.parse_trade = lambda trade: dict(trade)
    bybit.parse_position = lambda position: dict(position)

    first_order = bybit_client.future('orders')
    bybit.handle_order(bybit_client, {'data': [{'id': '1', 'symbol': 'BTC/USDT:USDT', 'status': 'open', 'category': 'linear'}]})
    bybit.handle_order(bybit_client, {'data': [{'id': '1', 'symbol': 'BTC/USDT:USDT', 'status': 'closed', 'category': 'linear'}]})
    assert (await first_order)[0]['status'] == 'open'
    assert (await bybit_client.future('orders'))[0]['status'] == 'closed'

    first_trade = bybit_client.future('myTrades')
    bybit.handle_my_trades(bybit_client, {'topic': 'execution', 'data': [{'id': '1', 'symbol': 'BTC/USDT:USDT', 'execType': 'Trade'}]})
    bybit.handle_my_trades(bybit_client, {'topic': 'execution', 'data': [{'id': '2', 'symbol': 'BTC/USDT:USDT', 'execType': 'Trade'}]})
    assert [trade['id'] for trade in await first_trade] == ['1']
    assert [trade['id'] for trade in await bybit_client.future('myTrades')] == ['2']

    first_position = bybit_client.future('positions')
    bybit.handle_positions(bybit_client, {'data': [{'symbol': 'BTC/USDT:USDT', 'side': 'long', 'contracts': 1}]})
    bybit.handle_positions(bybit_client, {'data': [{'symbol': 'BTC/USDT:USDT', 'side': 'long', 'contracts': 2}]})
    assert (await first_position)[0]['contracts'] == 1
    assert (await bybit_client.future('positions'))[0]['contracts'] == 2

    binance = ccxtpro.binance({'newUpdates': True})
    binance.newUpdates = True
    binance_client = create_client(hashes=['orders', 'myTrades', 'future:positions'])
    binance_client.subscriptions['future'] = {}
    binance.parse_ws_order = lambda order: dict(order)
    binance.parse_ws_trade = lambda trade: dict(trade)
    binance.parse_ws_position = lambda position: dict(position)

    first_order = binance_client.future('orders')
    binance.handle_order(binance_client, {'id': '1', 'symbol': 'BTC/USDT:USDT', 'status': 'open'})
    binance.handle_order(binance_client, {'id': '1', 'symbol': 'BTC/USDT:USDT', 'status': 'closed'})
    assert (await first_order)[0]['status'] == 'open'
    assert (await binance_client.future('orders'))[0]['status'] == 'closed'

    first_trade = binance_client.future('myTrades')
    binance.handle_my_trade(binance_client, {'x': 'TRADE', 'id': '1', 'order': 'trade-order', 'symbol': 'BTC/USDT:USDT', 'fee': {'currency': 'USDT', 'cost': 1}})
    binance.handle_my_trade(binance_client, {'x': 'TRADE', 'id': '2', 'order': 'trade-order', 'symbol': 'BTC/USDT:USDT', 'fee': {'currency': 'USDT', 'cost': 1}})
    assert [trade['id'] for trade in await first_trade] == ['1']
    assert [trade['id'] for trade in await binance_client.future('myTrades')] == ['2']

    first_position = binance_client.future('future:positions')
    binance.handle_positions(binance_client, {'E': 1, 'a': {'P': [{'symbol': 'BTC/USDT:USDT', 'side': 'long', 'contracts': 1}]}})
    binance.handle_positions(binance_client, {'E': 2, 'a': {'P': [{'symbol': 'BTC/USDT:USDT', 'side': 'long', 'contracts': 2}]}})
    assert (await first_position)[0]['contracts'] == 1
    assert (await binance_client.future('future:positions'))[0]['contracts'] == 2


async def test_message_queue_lifecycle():
    try:
        create_client(capacity=0)
        assert False, 'Expected invalid queue capacity'
    except ValueError:
        pass

    client = create_client(capacity=100)
    cancelled = client.future('positions')
    cancelled.cancel()
    for index in range(100):
        payload = [{'index': index}]
        client.resolve(payload, 'positions')
        payload[0]['index'] = -1
    for index in range(100):
        assert await client.future('positions') == [{'index': index}]
    assert client.messageQueueSize == 0
    assert client.messageQueue == {}

    client = create_client(capacity=2, hashes=['orders', 'positions', 'myTrades'])
    pending = client.future('myTrades')
    client.resolve([{'id': 'order'}], 'orders')
    client.resolve([{'id': 'position'}], 'positions')
    client.resolve([{'id': 'overflow'}], 'orders')
    results = await asyncio.gather(pending, client.future('positions'), return_exceptions=True)
    assert all(isinstance(result, NetworkError) for result in results)
    client.resolve([{'id': 'ignored'}], 'positions')
    assert client.messageQueueSize == 0
    assert client.messageQueue == {}

    client = create_client(hashes=['orders', 'positions'])
    client.resolve([{'id': 'stale'}], 'orders')
    client.resolve([{'id': 'retained'}], 'positions')
    error = NetworkError('order subscription rejected')
    client.reject(error, 'orders')
    assert client.messageQueueSize == 1
    rejected = client.future('orders')
    assert rejected.exception() is error
    assert await client.future('positions') == [{'id': 'retained'}]
    assert client.messageQueueSize == 0

    client = create_client(hashes=['orders', 'positions'])
    pending = client.future('orders')
    client.resolve([{'id': 'stale'}], 'positions')
    client.reset({'code': 500, 'message': 'reset'})
    assert isinstance(pending.exception(), NetworkError)
    assert client.messageQueue == {}
    assert client.messageQueueSize == 0

    client = create_client(hashes=['orders', 'positions'])
    pending = client.future('orders')
    client.resolve([{'id': 'stale'}], 'positions')
    await client.close()
    assert pending.cancelled()
    assert client.messageQueue == {}
    assert client.messageQueueSize == 0

    replacement = create_client()
    assert replacement.messageQueueError is None
    replacement.resolve([{'id': 'fresh'}], 'positions')
    assert await replacement.future('positions') == [{'id': 'fresh'}]
    await replacement.close()


async def test_bingx_private_batches(number=float):
    exchange = ccxtpro.bingx({'newUpdates': True, 'options': {
        'defaultType': 'swap', 'listenKey': 'test-listen-key',
        'ws': {'messageQueueHashes': ['swap:order', 'swap:mytrades', 'swap:positions'], 'messageQueueCapacity': 10},
    }})
    exchange.number = number
    exchange.newUpdates = True
    exchange.set_markets([{
        'id': 'ETH-USDT', 'symbol': 'ETH/USDT:USDT', 'base': 'ETH',
        'quote': 'USDT', 'settle': 'USDT', 'type': 'swap', 'spot': False,
        'swap': True, 'linear': True, 'inverse': False, 'contract': True,
        'contractSize': 1, 'precision': {'amount': 0.001, 'price': 0.01},
    }])
    client = exchange.client('wss://example.invalid')

    async def authenticate(params=None):
        return None

    async def watch(url, message_hash, *args):
        return await client.future(message_hash)

    exchange.authenticate = authenticate
    exchange.watch = watch
    raw = {
        's': 'ETH-USDT', 'i': 'order-1', 'c': 'client-1', 'S': 'BUY',
        'o': 'MARKET', 'q': '0.004', 'p': '2716', 'ap': '2713.85',
        'x': 'TRADE', 'X': 'FILLED', 'N': 'USDT', 'n': '-0.005',
        'T': 1000, 'ps': 'LONG', 'z': '0.004', 'td': 0,
    }
    first_order = client.future('swap:order')
    exchange.handle_message(client, {'e': 'ORDER_TRADE_UPDATE', 'E': 1001, 'o': raw})
    exchange.handle_message(client, {'e': 'ORDER_TRADE_UPDATE', 'E': 1002, 'o': {**raw, 'i': 'order-2'}})
    assert (await first_order)[0]['id'] == 'order-1'
    assert (await exchange.watch_orders())[0]['id'] == 'order-2'
    assert 'swap:mytrades' not in client.messageQueue

    first_trade = client.future('swap:mytrades')
    exchange.handle_message(client, {'e': 'TRADE_UPDATE', 'E': 1001, 'o': {**raw, 'td': 11}})
    exchange.handle_message(client, {'e': 'TRADE_UPDATE', 'E': 1002, 'o': {**raw, 'td': 12, 'X': 'PARTIALLY_FILLED', 'l': '0.001', 'L': '2700'}})
    incomplete = (await first_trade)[0]
    assert incomplete['id'] == '11'
    assert incomplete['amount'] is None and incomplete['price'] is None
    assert incomplete['info']['z'] == '0.004'
    fill = (await exchange.watch_my_trades())[0]
    assert fill['id'] == '12' and float(fill['amount']) == 0.001 and float(fill['price']) == 2700
    assert float(fill['cost']) == 2.7
    cumulative = (await client.future('swap:order'))[0]
    assert float(cumulative['filled']) == 0.004
    assert cumulative['info']['td'] == 11
    assert (await client.future('swap:order'))[0]['info']['td'] == 12

    first_position = client.future('swap:positions')
    for quantity in ('0.004', '0'):
        exchange.handle_positions(client, {'E': 1000, 'a': {'P': [{'s': 'ETH-USDT', 'pa': quantity, 'ps': 'LONG', 'ep': '2700'}]}})
    assert float((await first_position)[0]['contracts']) == 0.004
    assert float((await client.future('swap:positions'))[0]['contracts']) == 0
    pending = client.future('swap:order')
    exchange.handle_message(client, {'e': 'listenKeyExpired'})
    assert isinstance(pending.exception(), NetworkError)
    assert exchange.options['listenKey'] is None
    await exchange.close()

    ordinary = ccxtpro.bingx({'newUpdates': True})
    ordinary.set_markets(list(exchange.markets.values()))
    ordinary_client = ordinary.client('wss://example.invalid')
    result = ordinary_client.future('swap:order')
    ordinary.handle_message(ordinary_client, {'e': 'ORDER_TRADE_UPDATE', 'E': 1001, 'o': raw})
    assert await result is ordinary.orders
    assert not ordinary_client.message_queue_enabled('swap:order')
    await ordinary.close()


if __name__ == '__main__':
    asyncio.run(test_message_queue())
    asyncio.run(test_private_stream_batches())
    asyncio.run(test_message_queue_lifecycle())
    asyncio.run(test_bingx_private_batches())
    asyncio.run(test_bingx_private_batches(str))
