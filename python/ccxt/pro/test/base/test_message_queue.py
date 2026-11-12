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
    binance_client = create_client(hashes=['orders', 'myTrades', 'linear:positions'])
    binance_client.subscriptions['linear'] = {}
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

    first_position = binance_client.future('linear:positions')
    binance.handle_positions(binance_client, {'E': 1, 'a': {'P': [{'symbol': 'BTC/USDT:USDT', 'side': 'long', 'contracts': 1}]}})
    binance.handle_positions(binance_client, {'E': 2, 'a': {'P': [{'symbol': 'BTC/USDT:USDT', 'side': 'long', 'contracts': 2}]}})
    assert (await first_position)[0]['contracts'] == 1
    assert (await binance_client.future('linear:positions'))[0]['contracts'] == 2


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


async def test_private_stream_start():
    from ccxt.async_support.base.ws.cache import ArrayCacheBySymbolById

    async def authenticate(*args, **kwargs):
        return None

    for exchange_class in (ccxtpro.bybit, ccxtpro.binance, ccxtpro.bingx):
        for suffix in ('orders', 'my_trades', 'positions'):
            owner = exchange_class({'newUpdates': True, 'enableRateLimit': False})
            owner.newUpdates = True
            owner.markets = {}
            owner.authenticate = authenticate
            owner.options.update({
                'defaultType': 'swap', 'defaultSubType': 'linear',
                'listenKey': 'offline', 'linear': {'listenKey': 'offline'},
                'future': {'listenKey': 'offline'},
                'watchPositions': {'fetchPositionsSnapshot': False, 'awaitPositionsSnapshot': False},
            })
            url = 'wss://example.invalid/private'

            async def market_url(*args, **kwargs):
                return url
            owner.get_url_by_market_type = market_url
            owner.get_private_type = lambda _url: 'unified'
            owner.get_private_ws_url = lambda *args: url
            owner.set_balance_cache = lambda *args: None
            if owner.id == 'bingx':
                url = owner.urls['api']['ws']['linear'] + '?listenKey=offline'
            client = owner.client(url)
            client.connected.resolve(url)
            sent = []

            async def send(message):
                sent.append(message)
            client.send = send
            try:
                preparation = await getattr(owner, 'prepare_' + suffix + '_subscription')()
                message_hash = preparation.get('messageHash') or preparation['messageHashes'][0]
                client.messageQueueHashes = {message_hash}
                client.messageQueueCapacity = 10
                cached = ArrayCacheBySymbolById(10)
                cached.append({'id': 'first', 'symbol': 'BTC/USDT:USDT', 'side': 'long'})
                client.resolve(cached, message_hash)
                await asyncio.wait_for(getattr(owner, 'start_' + suffix)(), 1)
                assert message_hash not in client.futures
                assert client.messageQueueSize == 1
                first_send_count = len(sent)
                await asyncio.wait_for(getattr(owner, 'start_' + suffix)(), 1)
                assert len(sent) == first_send_count
                assert client.messageQueueSize == 1
                received = await asyncio.wait_for(getattr(owner, 'watch_' + suffix)(), 1)
                assert [item['id'] for item in received] == ['first']
                assert client.messageQueueSize == 0
                assert len(sent) == first_send_count
                assert not client.pending_subscription_tasks
            finally:
                await owner.close()


async def test_subscription_start_lifecycle():
    from ccxt.async_support.base.exchange import Exchange

    for cancel_caller in (False, True):
        owner = Exchange()
        client = create_client(hashes=['orders'])
        client.connected.resolve(client.url)
        owner.clients[client.url] = client
        entered = asyncio.Event()
        released = asyncio.Event()
        sent = []

        async def throttle(cost):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                released.set()

        async def send(message):
            sent.append(message)
        client.throttle = throttle
        client.send = send
        startup = asyncio.create_task(owner.start_subscription(client.url, {'op': 'subscribe'}, ['orders']))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            if cancel_caller:
                startup.cancel()
                await asyncio.gather(startup, return_exceptions=True)
                assert not client.connected.cancelled()
                assert len(client.pending_subscription_tasks) == 1
            await asyncio.wait_for(client.close(), 1)
            assert released.is_set()
            assert not sent
            assert not client.pending_subscription_tasks
            if not cancel_caller:
                outcome = (await asyncio.gather(startup, return_exceptions=True))[0]
                assert isinstance(outcome, NetworkError), type(outcome)
        finally:
            await owner.close()

    owner = Exchange({'enableRateLimit': False})
    client = create_client(capacity=1, hashes=['orders'])
    client.connected.resolve(client.url)
    owner.clients[client.url] = client

    async def send(message):
        client.resolve([{'id': 'first'}, {'id': 'second'}], 'orders')
    client.send = send
    try:
        received = await asyncio.wait_for(owner.watch(client.url, 'orders', {'op': 'subscribe'}, 'orders'), 1)
        assert len(received) == 2
        assert client.messageQueueError is None
    finally:
        await owner.close()


async def test_startup_task_cancelled_before_send_step():
    from ccxt.async_support.base.exchange import Exchange

    owner = Exchange({'enableRateLimit': False})
    client = create_client(hashes=['orders'])
    client.connected.resolve(client.url)
    owner.clients[client.url] = client
    sent = []

    async def send(message):
        sent.append(message)
    client.send = send
    startup = asyncio.create_task(owner.start_subscription(client.url, {'op': 'subscribe'}, ['orders']))
    try:
        await asyncio.sleep(0)
        await client.close()
        outcome = (await asyncio.gather(startup, return_exceptions=True))[0]
        assert isinstance(outcome, NetworkError), type(outcome)
        assert not sent
        assert not client.pending_subscription_tasks
    finally:
        await owner.close()
