// NO_AUTO_TRANSPILE

import assert from 'node:assert/strict';
import Exchange from '../../../base/Exchange.js';
import { OperationFailed } from '../../../base/errors.js';

export default async function testBeforeRestRequest () {
    const exchange = new Exchange ({ 'enableRateLimit': false });
    assert.equal (await exchange.beforeRestRequest ('probe'), undefined);
    const events: string[] = [];
    exchange.beforeRestRequest = async (path, api, method, params) => {
        await Promise.resolve ();
        assert.equal (path, 'probe');
        assert.equal (api, 'public');
        assert.equal (method, 'GET');
        assert.deepEqual (params, { 'symbol': 'BTCUSDT' });
        events.push ('hook');
        return undefined;
    };
    exchange.sign = () => {
        events.push ('sign');
        return { 'url': 'https://example.invalid', 'method': 'GET', 'headers': {}, 'body': undefined };
    };
    let attempts = 0;
    exchange.fetch = async () => {
        events.push ('fetch');
        if (++attempts === 1) {
            throw new OperationFailed ('retry');
        }
        return 'OK';
    };
    assert.equal (await exchange.fetch2 ('probe', 'public', 'GET', {
        'symbol': 'BTCUSDT', 'maxRetriesOnFailure': 1,
    }), 'OK');
    assert.deepEqual (events, [ 'hook', 'sign', 'fetch', 'hook', 'sign', 'fetch' ]);
    const failure = new OperationFailed ('guard failure');
    exchange.beforeRestRequest = async () => { throw failure; };
    await assert.rejects (exchange.fetch2 ('probe', 'public', 'GET', {
        'maxRetriesOnFailure': 2,
    }), error => error === failure);
    assert.equal (events.length, 6);
}
