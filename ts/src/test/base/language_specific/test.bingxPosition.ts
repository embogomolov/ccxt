// NO_AUTO_TRANSPILE
// @ts-nocheck

import assert from 'assert';
import ccxt from '../../../../ccxt.js';

async function testBingxPosition () {
    const exchange = new ccxt.bingx ();
    exchange.fetch = async () => { throw new Error ('Неожиданный сетевой запрос'); };
    for (const inverse of [ false, true ]) {
        const symbol = inverse ? 'BTC/USD:BTC' : 'BTC/USDT:USDT';
        const marketId = inverse ? 'BTC-USD' : 'BTC-USDT';
        exchange.markets = { [symbol]: { 'symbol': symbol, 'id': marketId, 'type': 'swap', 'swap': true, 'inverse': inverse, 'contractSize': 1 } };
        exchange.markets_by_id = { [marketId]: [ exchange.markets[symbol] ] };
        const endpoint = inverse ? 'cswapV1PrivateGetUserPositions' : 'swapV2PrivateGetUserPositions';
        const responses = [
            {},
            { 'data': null },
            { 'data': {} },
            { 'data': 'invalid' },
            { 'data': [ null ] },
            { 'data': [ {} ] },
            { 'data': [ { 'symbol': marketId } ] },
            { 'data': [ { 'symbol': 'ETH-USDT', 'positionAmt': '1' } ] },
            { 'data': [ { 'symbol': marketId, 'positionAmt': null } ] },
        ];
        for (const response of responses) {
            exchange[endpoint] = async () => response;
            await assert.rejects (exchange.fetchPosition (symbol), ccxt.ExchangeError);
        }
        const emptyResponse = { 'code': 0, 'data': [] };
        exchange[endpoint] = async () => emptyResponse;
        assert.deepStrictEqual (await exchange.fetchPosition (symbol), {
            'info': emptyResponse, 'symbol': symbol, 'contracts': 0, 'contractSize': 1,
        });
        for (const numberParser of [ Number, String ]) {
            exchange.number = numberParser;
            exchange[endpoint] = async () => ({ 'code': 0, 'data': [ { 'symbol': marketId, 'positionAmt': '0.16' } ] });
            const position = await exchange.fetchPosition (symbol);
            assert.strictEqual (position['symbol'], symbol);
            assert.strictEqual (position['contracts'], numberParser ('0.16'));
            exchange[endpoint] = async () => emptyResponse;
            const absent = await exchange.fetchPosition (symbol);
            assert.strictEqual (absent['contracts'], 0);
            assert.strictEqual (absent['symbol'], symbol);
        }
        exchange.number = Number;
    }
}

export default testBingxPosition;
