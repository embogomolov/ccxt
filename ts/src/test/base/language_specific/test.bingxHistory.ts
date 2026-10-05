// NO_AUTO_TRANSPILE
// @ts-nocheck

import assert from 'assert';
import ccxt from '../../../../ccxt.js';

async function testBingxHistory () {
    const exchange = new ccxt.bingx ();
    for (const [ code, msg, errorType ] of [
        [ 100410, 'Please try again later.', ccxt.RateLimitExceeded ],
        [ 100410, 'The current system is busy, please try again later', ccxt.RateLimitExceeded ],
        [ 100500, 'The current system is busy, please try again later', ccxt.OperationFailed ],
        [ 109400, 'invalid quantity', ccxt.BadRequest ],
        [ 109400, 'order not exist', ccxt.OrderNotFound ],
        [ 109421, 'order not exist', ccxt.OrderNotFound ],
        [ 110402, 'price invalid', ccxt.InvalidOrder ],
        [ 110411, 'SL price invalid', ccxt.InvalidOrder ],
        [ 110413, 'TP price invalid', ccxt.InvalidOrder ],
        [ 106551, 'GetFillOrdersListForApi count db has err:sql: no rows in result set', ccxt.OperationFailed ],
        [ 106551, 'unknown failure', ccxt.ExchangeError ],
    ]) {
        const response = { code, msg };
        assert.throws (() => exchange.handleErrors (200, '', '', 'GET', {}, JSON.stringify (response), response, {}, undefined), errorType);
    }
    const demoExchange = new ccxt.bingx ({ 'commonCurrencies': { 'VST': 'USDT' } });
    const demoBalance = { 'asset': 'VST', 'availableMargin': '10', 'usedMargin': '2' };
    assert.strictEqual (demoExchange.parseBalance ({ 'data': [ demoBalance ] }).USDT.free, 10);
    assert.throws (() => demoExchange.parseBalance ({ 'data': [ demoBalance, { ...demoBalance, 'asset': 'USDT' } ] }), ccxt.OperationFailed);
    assert.strictEqual (demoExchange.safeCurrencyCode ('VST'), 'USDT');
    assert.strictEqual (exchange.safeCurrencyCode ('VST'), 'VST');
    const symbol = 'ETH/USDT:USDT';
    const timestamp = 1790959600000;
    exchange.setMarkets ([ {
        'id': 'ETH-USDT', 'symbol': symbol, 'base': 'ETH', 'quote': 'USDT',
        'settle': 'USDT', 'spot': false, 'swap': true, 'linear': true,
        'inverse': false, 'contract': true, 'contractSize': 1, 'type': 'swap',
    } ]);
    exchange.milliseconds = () => timestamp + 10000;
    exchange.fetch = async () => { throw new Error ('unexpected network request'); };
    const fill = {
        'symbol': 'ETH-USDT', 'qty': '0.002', 'quoteQty': '5.3783', 'role': 'taker',
        'commission': '-0.002689', 'commissionAsset': 'VST', 'price': '2689.17',
        'orderId': '2106065676096507904', 'tradeId': '362625236',
        'filledTime': '2026-10-03T00:56:01.000+08:00', 'side': 'SELL', 'positionSide': 'BOTH',
    };
    const parsed = exchange.parseTrade (fill, exchange.market (symbol));
    assert.strictEqual (parsed.id, '362625236');
    assert.strictEqual (parsed.timestamp, Date.parse (fill.filledTime));
    assert.strictEqual (parsed.amount, 0.002);
    assert.strictEqual (parsed.cost, 5.3783);
    assert.strictEqual (parsed.fee.cost, 0.002689);
    assert.strictEqual (exchange.parseTrade ({ ...fill, 'commission': '0.001' }, exchange.market (symbol)).fee.cost, -0.001);
    for (const [ positionSide, side, reduceOnly, expected ] of [
        [ 'LONG', 'SELL', false, true ], [ 'SHORT', 'BUY', false, true ],
        [ 'LONG', 'BUY', false, false ], [ 'SHORT', 'SELL', false, false ],
        [ 'BOTH', 'SELL', false, false ], [ 'BOTH', 'SELL', true, true ],
    ]) {
        const raw = { 'orderId': '1', 'symbol': 'ETH-USDT', 'type': 'MARKET', 'positionSide': positionSide, 'side': side, 'reduceOnly': reduceOnly };
        const order = exchange.parseOrder (raw, exchange.market (symbol));
        assert.strictEqual (order.reduceOnly, expected);
        assert.strictEqual (raw.reduceOnly, reduceOnly);
    }
    const records = Array.from ({ length: 1001 }, (_, index) => ({
        ...fill, 'tradeId': String (index + 1), 'filledTime': new Date (timestamp + index).toISOString (),
    }));
    for (const inclusiveEnd of [ false, true ]) {
        exchange.swapV2PrivateGetTradeFillHistory = async (params) => {
            assert.strictEqual (params.pageIndex, 1);
            const matches = records.filter ((row) => {
                const time = Date.parse (row.filledTime);
                return time > params.startTs && (inclusiveEnd ? time <= params.endTs : time < params.endTs);
            });
            return { 'code': 0, 'data': { 'fill_history_orders': matches.slice (0, params.pageSize), 'total': matches.length } };
        };
        const trades = await exchange.fetchMyTrades (symbol, timestamp, undefined, { 'until': timestamp + 1000, 'paginate': true });
        assert.strictEqual (trades.length, 1001);
        assert.strictEqual (new Set (trades.map ((trade) => trade.id)).size, 1001);
    }
    exchange.swapV2PrivateGetTradeFillHistory = async () => ({
        'code': 0, 'data': { 'fill_history_orders': records.slice (0, 1000), 'total': 1001 },
    });
    await assert.rejects (exchange.fetchMyTrades (symbol, timestamp, undefined, { 'until': timestamp, 'paginate': true }), ccxt.OperationFailed);
    exchange.swapV2PrivateGetTradeFillHistory = async () => ({ 'code': 0, 'data': null });
    await assert.rejects (exchange.fetchMyTrades (symbol, timestamp, undefined, { 'until': timestamp, 'paginate': true }), ccxt.OperationFailed);
    const orders = Array.from ({ length: 1001 }, (_, index) => ({
        'orderId': String (index + 1), 'symbol': 'ETH-USDT', 'type': 'STOP_MARKET',
        'positionSide': 'BOTH', 'side': 'SELL', 'status': 'CANCELED', 'origQty': '0.002',
        'executedQty': '0', 'time': timestamp, 'clientOrderId': 'abcdef01_sl_0_0',
    }));
    exchange.swapV2PrivateGetTradeAllOrders = async (params) => ({
        'code': 0, 'data': { 'orders': orders.filter ((order) => Number (order.orderId) > Number (params.orderId ?? 0)).slice (0, params.limit) },
    });
    const history = await exchange.fetchOrders (symbol, timestamp, undefined, { 'until': timestamp + 1000, 'paginate': true });
    assert.strictEqual (history.length, 1001);
    assert.strictEqual (new Set (history.map ((order) => order.id)).size, 1001);
    const fundingRows = Array.from ({ length: 1001 }, (_, index) => ({
        'tranId': String (index + 1), 'time': timestamp + index, 'symbol': 'ETH-USDT',
        'incomeType': 'FUNDING_FEE', 'income': (index % 2 !== 0) ? '-0.1' : '0.1', 'asset': 'USDT',
    }));
    for (const inclusiveEnd of [ false, true ]) {
        exchange.swapV2PrivateGetUserIncome = async (params) => ({
            'code': 0,
            'data': fundingRows.filter ((row) => row.time > params.startTime && (inclusiveEnd ? row.time <= params.endTime : row.time < params.endTime)).slice (0, params.limit),
        });
        const funding = await exchange.fetchFundingHistory (symbol, timestamp, undefined, { 'endTime': timestamp + 1000, 'paginate': true });
        assert.strictEqual (funding.length, 1001);
        assert.strictEqual (new Set (funding.map ((row) => row.id)).size, 1001);
        assert.strictEqual (funding[0].amount, 0.1);
        assert.strictEqual (funding[1].amount, -0.1);
        await assert.rejects (exchange.fetchFundingHistory (symbol, timestamp, undefined, { 'until': timestamp + 1000, 'paginate': true, 'paginationCalls': 1 }), ccxt.OperationFailed);
    }
    exchange.swapV2PrivateGetUserIncome = async () => ({ 'code': 0, 'data': fundingRows.slice (0, 1000) });
    await assert.rejects (exchange.fetchFundingHistory (symbol, timestamp, undefined, { 'until': timestamp, 'paginate': true }), ccxt.OperationFailed);
    for (const response of [ { 'code': 0 }, { 'code': 0, 'data': {} }, { 'code': 0, 'data': [ { ...fundingRows[0], 'tranId': undefined } ] } ]) {
        exchange.swapV2PrivateGetUserIncome = async () => response;
        await assert.rejects (exchange.fetchFundingHistory (symbol, timestamp, undefined, { 'until': timestamp, 'paginate': true }), ccxt.OperationFailed);
    }
    exchange.swapV2PrivateGetUserIncome = async () => ({ 'code': 0, 'data': null });
    assert.deepStrictEqual (await exchange.fetchFundingHistory (symbol, timestamp, undefined, { 'until': timestamp, 'paginate': true }), []);
    await assert.rejects (exchange.fetchFundingHistory (symbol, undefined, undefined, { 'paginate': true }), ccxt.ArgumentsRequired);
    await exchange.close ();
}

export default testBingxHistory;
