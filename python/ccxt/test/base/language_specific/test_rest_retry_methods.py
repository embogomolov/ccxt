import asyncio
import inspect
from unittest.mock import AsyncMock, Mock

import ccxt
import ccxt.async_support as async_ccxt
import ccxt.pro as pro_ccxt


async def test_rest_retry_methods():
    for exchange_class in (ccxt.bingx, async_ccxt.bingx, pro_ccxt.bingx, ccxt.bybit, async_ccxt.binance):
        asynchronous = inspect.iscoroutinefunction(exchange_class.fetch2)
        mock = AsyncMock if asynchronous else Mock
        for methods in (None, ['GET']):
            for verb in ('GET', 'POST', 'PUT', 'DELETE', 'PATCH'):
                for retries in (0, 2):
                    options = {'maxRetriesOnFailure': 2}
                    if methods is not None:
                        options['maxRetriesOnFailureMethods'] = methods
                    exchange = exchange_class({'enableRateLimit': False, 'options': options})
                    events = []

                    def before_request(*_args):
                        events.append('hook')

                    def sign(_path, _api, method, params, *_args):
                        events.append('sign')
                        assert params == {'symbol': 'BTCUSDT'}
                        return {'url': 'https://example.invalid', 'method': method, 'headers': {}, 'body': None}

                    exchange.before_rest_request = mock(side_effect=before_request)
                    exchange.sign = sign
                    exchange.fetch = mock(side_effect=[ccxt.RateLimitExceeded('retry'), 'OK'])
                    allowed = retries > 0 and (methods is None or verb in methods)
                    try:
                        response = exchange.fetch2('same/path', 'public', verb, {
                            'symbol': 'BTCUSDT', 'maxRetriesOnFailure': retries,
                        })
                        response = await response if asynchronous else response
                        assert allowed and response == 'OK'
                    except ccxt.RateLimitExceeded:
                        assert not allowed
                    assert exchange.fetch.call_count == (2 if allowed else 1)
                    assert events == ['hook', 'sign'] * (2 if allowed else 1)
                    if asynchronous:
                        await exchange.close()
        for invalid in (None, 'GET', {}, [1]):
            exchange = exchange_class({
                'enableRateLimit': False, 'options': {'maxRetriesOnFailureMethods': invalid},
            })
            exchange.fetch = mock()
            try:
                response = exchange.fetch2('same/path')
                if asynchronous:
                    await response
                raise AssertionError('Invalid retry policy is accepted')
            except ccxt.BadRequest:
                exchange.fetch.assert_not_called()
            if asynchronous:
                await exchange.close()


if __name__ == '__main__':
    asyncio.run(test_rest_retry_methods())
    print('PASS')
