// NO_AUTO_TRANSPILE

import assert from 'node:assert/strict';
import Exchange from '../../../base/Exchange.js';
import { BadRequest, OperationFailed, RateLimitExceeded } from '../../../base/errors.js';

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

    for (const method of [ 'GET', 'POST', 'PUT', 'DELETE', 'PATCH' ]) {
        const guarded = new Exchange ({
            'enableRateLimit': false,
            'options': { 'maxRetriesOnFailure': 3, 'maxRetriesOnFailureMethods': [ 'GET' ] },
        });
        const attempts: string[] = [];
        guarded.beforeRestRequest = async () => { attempts.push ('hook'); return undefined; };
        guarded.sign = (_path, _api, verb, params) => {
            assert.deepEqual (params, { 'symbol': 'BTCUSDT' });
            attempts.push ('sign');
            return { 'url': 'https://example.invalid', 'method': verb };
        };
        let count = 0;
        guarded.fetch = async () => {
            attempts.push ('fetch');
            if (++count === 1) {
                throw new RateLimitExceeded ('retry');
            }
            return 'OK';
        };
        const request = guarded.fetch2 ('probe', 'public', method, {
            'symbol': 'BTCUSDT', 'maxRetriesOnFailure': 3,
        });
        if (method === 'GET') {
            assert.equal (await request, 'OK');
            assert.deepEqual (attempts, [ 'hook', 'sign', 'fetch', 'hook', 'sign', 'fetch' ]);
        } else {
            await assert.rejects (request, RateLimitExceeded);
            assert.deepEqual (attempts, [ 'hook', 'sign', 'fetch' ]);
        }
        count = 0;
        await assert.rejects (guarded.fetch2 ('probe', 'public', 'GET', {
            'symbol': 'BTCUSDT', 'maxRetriesOnFailure': 0,
        }), RateLimitExceeded);
        assert.equal (count, 1);
    }
    for (const invalid of [ null, 'GET', {}, [ 1 ] ]) {
        const guarded = new Exchange ({
            'enableRateLimit': false, 'options': { 'maxRetriesOnFailureMethods': invalid },
        });
        guarded.fetch = async () => { assert.fail ('invalid policy reaches HTTP'); };
        await assert.rejects (guarded.fetch2 ('probe'), BadRequest);
    }
}
