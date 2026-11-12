// NO_AUTO_TRANSPILE
import assert from 'node:assert/strict';
import test from 'node:test';
import Exchange from '../../../base/Exchange.js';
import WsClient from '../../../base/ws/WsClient.js';
import { Future, type FutureInterface } from '../../../base/ws/Future.js';
import { NetworkError } from '../../../base/errors.js';

function harness () {
    const exchange = new Exchange ({ 'enableRateLimit': false });
    const url = 'wss://example.invalid/private';
    const noop = () => {};
    const client = new WsClient (url, noop, exchange.onError.bind (exchange), exchange.onClose.bind (exchange), noop);
    const connected = Future ();
    connected.resolve (url);
    client.connected = connected;
    client.connect = () => connected;
    const sent: any[] = [];
    client.send = async (message: any) => { sent.push (message); };
    exchange.clients[url] = client as any;
    return { exchange, client, url, sent };
}

function nextTurn () {
    return new Promise<void> ((resolve) => setImmediate (resolve));
}

function trackSubscriptions (future: any) {
    const subscribe = future.subscribe.bind (future);
    let active = 0;
    future.subscribe = (onFulfil: (value: any) => void, onReject: (error: any) => void) => {
        active++;
        const unsubscribe = subscribe (onFulfil, onReject);
        let subscribed = true;
        return () => {
            if (subscribed) {
                subscribed = false;
                active--;
            }
            unsubscribe ();
        };
    };
    return () => active;
}

test ('control startup keeps data ownership with the consumer', async () => {
    const { exchange, client, url, sent } = harness ();
    await exchange.startSubscription (url, { 'op': 'subscribe' }, [ 'orders' ]);
    assert.equal (client.futures['orders'], undefined);
    assert.equal (sent.length, 1);
    await exchange.startSubscription (url, { 'op': 'subscribe' }, [ 'orders' ]);
    assert.equal (sent.length, 1);
    const received = exchange.watch (url, 'orders', { 'op': 'subscribe' }, 'orders');
    client.resolve ([ { 'id': 'first' } ], 'orders');
    assert.deepEqual (await received, [ { 'id': 'first' } ]);
    assert.equal (sent.length, 1);
    assert.equal (client.pendingSubscriptionTasks.size, 0);
});

test ('watch registers its data waiter before sending', async () => {
    const { exchange, client, url } = harness ();
    client.send = async () => { client.resolve ([ { 'id': 'first' } ], 'orders'); };
    const received = exchange.watch (url, 'orders', { 'op': 'subscribe' }, 'orders');
    assert.deepEqual (await received, [ { 'id': 'first' } ]);
});

test ('connection closure settles throttled control work', async () => {
    const { exchange, client, url, sent } = harness ();
    exchange.enableRateLimit = true;
    const entered = Future ();
    client.throttle = async () => {
        entered.resolve (true);
        await new Promise (() => {});
    };
    const startup = exchange.startSubscription (url, { 'op': 'subscribe' }, [ 'orders' ]);
    await entered;
    client.onClose ({ 'code': 1000 });
    await assert.rejects (startup, NetworkError);
    await Promise.allSettled (Array.from (client.pendingSubscriptionTasks));
    assert.equal (sent.length, 0);
    assert.equal (client.pendingSubscriptionTasks.size, 0);
});

for (const failurePoint of [ 'connect', 'throttle', 'send' ]) {
    test ('watchMultiple without subscription hashes reports ' + failurePoint + ' failure and detaches', async () => {
        const { exchange, client, url } = harness ();
        const failure = new NetworkError (failurePoint);
        const subscribers = trackSubscriptions (client.future ('orders'));
        if (failurePoint === 'connect') {
            client.connect = () => Promise.reject (failure);
        } else if (failurePoint === 'throttle') {
            exchange.enableRateLimit = true;
            client.throttle = async () => { throw failure; };
        } else {
            client.send = async () => { throw failure; };
        }
        await assert.rejects (exchange.watchMultiple (url, [ 'orders' ], { 'op': 'subscribe' }), (error) => error === failure);
        await Promise.allSettled (Array.from (client.pendingSubscriptionTasks));
        assert.equal (subscribers (), 0);
        assert.equal (client.pendingSubscriptionTasks.size, 0);
        await client.close ();
    });
}

test ('anonymous startup waits for send and propagates its failure without a data waiter', async () => {
    const { exchange, client, url } = harness ();
    const entered = Future ();
    const sending = Future ();
    client.send = () => { entered.resolve (true); return sending; };
    const startup = exchange.startSubscription (url, { 'op': 'subscribe' });
    let settled = false;
    startup.then (() => { settled = true; }, () => { settled = true; });
    await entered;
    await nextTurn ();
    assert.equal (settled, false);
    assert.deepEqual (Object.keys (client.futures), []);
    const failure = new NetworkError ('send');
    sending.reject (failure);
    await assert.rejects (startup, (error) => error === failure);
    assert.equal (client.pendingSubscriptionTasks.size, 0);
});

test ('explicit empty hashes and missing messages preserve no-send behavior', async () => {
    const { exchange, url, sent } = harness ();
    await exchange.startSubscription (url, { 'op': 'subscribe' }, []);
    await exchange.startSubscription (url);
    await exchange.startSubscription (url, null);
    assert.equal (sent.length, 0);
});

test ('shared and mixed subscription hashes wait only for their own sends', async () => {
    const { exchange, client, url, sent } = harness ();
    const sends: Record<string, FutureInterface> = { 'orders': Future (), 'trades': Future (), 'positions': Future () };
    client.send = (message) => { sent.push (message); return sends[message.channel]; };
    const first = exchange.scheduleSubscription (client, url, { 'channel': 'orders' }, [ 'orders', 'orders:alias' ], undefined, 0);
    const repeated = exchange.scheduleSubscription (client, url, { 'channel': 'orders' }, [ 'orders', 'orders:alias' ], undefined, 0);
    const unrelated = exchange.scheduleSubscription (client, url, { 'channel': 'positions' }, [ 'positions' ], undefined, 0);
    const mixed = exchange.scheduleSubscription (client, url, { 'channel': 'trades' }, [ 'orders', 'trades' ], undefined, 0);
    assert.equal (first.pending.length, 1);
    assert.deepEqual (repeated.pending, first.pending);
    assert.equal (mixed.pending.length, 2);
    assert.ok (mixed.pending.includes (first.pending[0]));
    assert.ok (!mixed.pending.includes (unrelated.pending[0]));
    const noMessage = exchange.scheduleSubscription (client, url, undefined, [ 'trades' ], undefined, 0);
    assert.equal (noMessage.pending[0], client.subscriptionTasks['trades']);
    sends['orders'].resolve (true);
    sends['trades'].resolve (true);
    await Promise.all (mixed.pending);
    assert.equal (client.pendingSubscriptionTasks.size, 1);
    sends['positions'].resolve (true);
    await Promise.all (unrelated.pending);
    assert.equal (sent.length, 3);
    assert.equal (client.pendingSubscriptionTasks.size, 0);
});

test ('replacement operation does not wait for an obsolete hash mapping', async () => {
    const { exchange, client, url } = harness ();
    const sends: Record<string, FutureInterface> = { 'first': Future (), 'second': Future () };
    client.send = (message) => sends[message.name];
    const first = exchange.scheduleSubscription (client, url, { 'name': 'first' }, [ 'orders' ], undefined, 0);
    delete client.subscriptions['orders'];
    const second = exchange.scheduleSubscription (client, url, { 'name': 'second' }, [ 'orders' ], undefined, 0);
    assert.equal (second.pending.length, 1);
    assert.notEqual (second.pending[0], first.pending[0]);
    sends['first'].resolve (true);
    await Promise.all (first.pending);
    assert.equal (client.subscriptionTasks['orders'], second.pending[0]);
    sends['second'].resolve (true);
    await Promise.all (second.pending);
    assert.equal (client.pendingSubscriptionTasks.size, 0);
});

for (const settlement of [ 'resolve', 'reject' ] as const) {
    test ('external race ' + settlement + ' detaches inputs and preserves another observer', async () => {
        const source = Future ();
        const subscribers = trackSubscriptions (source);
        const first = Future.race ([ source ]);
        const second = Future.race ([ source ]);
        const value = (settlement === 'resolve') ? 'external' : new Error ('external');
        const observed = (settlement === 'resolve') ? first : assert.rejects (first, (error) => error === value);
        first[settlement] (value);
        assert.equal (subscribers (), 1);
        source.resolve ('payload');
        await observed;
        assert.equal (await second, 'payload');
        assert.equal (subscribers (), 0);
    });
}

test ('data race keeps synchronous delivery and releases an already-settled winner', async () => {
    const source = Future ();
    const result: any = Future.race ([ source ]);
    let delivered = false;
    result.subscribe (() => { delivered = true; }, assert.fail);
    source.resolve ('payload');
    assert.equal (delivered, true);
    assert.equal (await result, 'payload');
    const loser = Future ();
    const loserSubscribers = trackSubscriptions (loser);
    const winnerSubscribers = trackSubscriptions (source);
    assert.equal (await Future.race ([ loser, source ]), 'payload');
    assert.equal (loserSubscribers (), 0);
    assert.equal (winnerSubscribers (), 0);
    loser.reject (new Error ('late loser'));
});

test ('race setup failure releases registered inputs and keeps the original exception', () => {
    const source = Future ();
    const subscribers = trackSubscriptions (source);
    const failure = new Error ('subscribe');
    const broken = { 'subscribe': () => { throw failure; } };
    assert.throws (() => Future.race ([ source, broken ]), (error) => error === failure);
    assert.equal (subscribers (), 0);
    source.reject (new Error ('late source'));
});

test ('failed Promise observation does not leave a poisoned cache entry', async () => {
    const source = Promise.resolve ('payload');
    const then = source.then;
    const failure = new Error ('then');
    source.then = () => { throw failure; };
    assert.throws (() => Future.raceAsync ([ source ]), (error) => error === failure);
    source.then = then;
    assert.equal (await Future.raceAsync ([ source ]), 'payload');
});

test ('repeated startup observes the close signal once and keeps no completed tasks', async () => {
    const { exchange, client, url, sent } = harness ();
    const then = client.subscriptionClosed.then;
    let observations = 0;
    client.subscriptionClosed.then = function (this: Promise<any>, ...args: Parameters<typeof then>) {
        observations++;
        return then.apply (this, args);
    } as typeof then;
    for (let iteration = 0; iteration < 100; iteration++) {
        await exchange.startSubscription (url, { 'op': 'subscribe' }, [ 'orders' ]);
        assert.equal (client.pendingSubscriptionTasks.size, 0);
        assert.deepEqual (Object.keys (client.subscriptionTasks), []);
    }
    assert.equal (observations, 1);
    assert.equal (sent.length, 1);
    await client.close ();
});

test ('repeated failed watches do not retain data observers', async () => {
    const { exchange, client, url } = harness ();
    const subscribers = trackSubscriptions (client.future ('orders'));
    const failure = new NetworkError ('send');
    client.send = async () => { throw failure; };
    for (let iteration = 0; iteration < 100; iteration++) {
        await assert.rejects (exchange.watchMultiple (url, [ 'orders' ], { 'op': 'subscribe' }), (error) => error === failure);
        assert.equal (subscribers (), 0);
        assert.equal (client.pendingSubscriptionTasks.size, 0);
    }
    await client.close ();
});

test ('close interrupts a throttle Future that adopts a pending Promise', async () => {
    const { exchange, client, url, sent } = harness ();
    exchange.enableRateLimit = true;
    const adopted = Future ();
    const throttle = Future ();
    throttle.resolve (adopted);
    const entered = Future ();
    client.throttle = () => { entered.resolve (true); return throttle; };
    const startup = exchange.startSubscription (url, { 'op': 'subscribe' }, [ 'orders' ]);
    const rejected = assert.rejects (startup);
    await entered;
    await client.close ();
    await rejected;
    assert.equal (client.pendingSubscriptionTasks.size, 0);
    assert.equal (sent.length, 0);
    adopted.reject (new Error ('late throttle'));
    await nextTurn ();
});

test ('shared pending throttle has one observer across closed clients', async () => {
    const throttle = Future ();
    const then = throttle.then;
    let observations = 0;
    throttle.then = function (this: Promise<any>, ...args: Parameters<typeof then>) {
        observations++;
        return then.apply (this, args);
    } as typeof then;
    for (let iteration = 0; iteration < 20; iteration++) {
        const { exchange, client, url, sent } = harness ();
        exchange.enableRateLimit = true;
        const entered = Future ();
        client.throttle = () => { entered.resolve (true); return throttle; };
        const startup = exchange.startSubscription (url, { 'op': 'subscribe' }, [ 'orders' ]);
        const rejected = assert.rejects (startup);
        await entered;
        await client.close ();
        await rejected;
        assert.equal (sent.length, 0);
        assert.equal (client.pendingSubscriptionTasks.size, 0);
    }
    assert.equal (observations, 1);
    throttle.reject (new Error ('late throttle'));
    await nextTurn ();
});

for (const settlement of [ 'resolve', 'reject' ] as const) {
    test ('Promise cache expires before reentrant ' + settlement + ' observation', async () => {
        let settle!: (value: any) => void;
        const source = new Promise ((resolve, reject) => {
            settle = (settlement === 'resolve') ? resolve : reject;
        });
        const first: any = Future.raceAsync ([ source ]);
        const observed = (settlement === 'reject') ? assert.rejects (first) : first;
        let reentrant;
        const observe = () => {
            const ready = Future ();
            ready.resolve ('synchronous');
            reentrant = Future.race ([ source, ready ]);
        };
        first.subscribe (observe, observe);
        settle ((settlement === 'resolve') ? 'native' : new Error ('native'));
        await observed;
        await nextTurn ();
        assert.equal (await reentrant, 'synchronous');
    });
}

test ('Promise normalization preserves thenable observations and settled-input order', async () => {
    let count = 0;
    const thenable = { 'then': (resolve: (value: number) => void) => { resolve (++count); } };
    assert.equal (await Future.raceAsync ([ thenable ]), 1);
    assert.equal (await Future.raceAsync ([ thenable ]), 2);
    const rejected = Promise.reject (new Error ('rejected'));
    await assert.rejects (Future.raceAsync ([ rejected ]));
    assert.equal (await Future.raceAsync ([ Promise.resolve ('first'), rejected ]), 'first');
});
