import assert from 'assert';
import bybit from '../../bybit.js';
import { ExchangeError } from '../../../base/errors.js';

async function testBybitSubscription () {
    for (const dataFirst of [ false, true ]) {
        for (const withAcknowledgement of [ false, true ]) {
            const exchange = new bybit ({ 'enableRateLimit': false });
            const url = 'wss://subscription-test';
            const client = exchange.client (url);
            const requestId = 'scanner-request';
            const oldSubscription = { 'id': 'old-request', 'topics': [ 'old' ] };
            client.subscriptions['old'] = oldSubscription;
            let sentRequest: any;
            exchange.watchMultiple = (requestUrl, hashes, message, subscribeHashes, subscription) => {
                assert.strictEqual (requestUrl, url);
                sentRequest = message;
                assert (subscribeHashes !== undefined);
                for (const messageHash of subscribeHashes) {
                    client.subscriptions[messageHash] = subscription;
                }
                return client.future (hashes[0]);
            };
            const acknowledgement = withAcknowledgement ? client.future (requestId) : undefined;
            const acknowledgementOutcome = withAcknowledgement ? Promise.allSettled ([ acknowledgement ]) : undefined;
            const watch = exchange.watchTopics (url, [ 'added' ], [ 'topic' ], { 'req_id': requestId });
            const watchOutcome = Promise.allSettled ([ watch ]);
            assert.strictEqual (sentRequest['req_id'], requestId);
            assert.strictEqual (client.subscriptions['added']['id'], sentRequest['req_id']);
            if (dataFirst) {
                client.resolve ('candle', 'added');
                assert.strictEqual (await watch, 'candle');
            }
            exchange.handleMessage (client, {
                'op': 'subscribe',
                'req_id': requestId,
                'success': false,
                'ret_msg': 'denied',
            });
            const outcomes = await watchOutcome;
            assert.strictEqual (outcomes[0].status, dataFirst ? 'fulfilled' : 'rejected');
            if (outcomes[0].status === 'rejected') {
                assert (outcomes[0].reason instanceof ExchangeError);
            }
            assert.deepStrictEqual (client.subscriptions, { 'old': oldSubscription });
            assert (!(requestId in client.rejections));
            if (acknowledgementOutcome !== undefined) {
                assert (!(requestId in client.futures));
                const acknowledgements = await acknowledgementOutcome;
                assert.strictEqual (acknowledgements[0].status, 'rejected');
                if (acknowledgements[0].status === 'rejected') {
                    assert (acknowledgements[0].reason instanceof ExchangeError);
                    if (dataFirst) {
                        assert.strictEqual (client.rejections['added'], acknowledgements[0].reason);
                    }
                }
            }
            await exchange.close ();
        }
    }
}

export default testBybitSubscription;
