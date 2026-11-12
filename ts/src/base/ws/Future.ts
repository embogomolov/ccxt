// @ts-nocheck
export interface FutureInterface extends Promise<any> {
    resolve(value: unknown): void;
    reject(reason?: any): void;
}

export function Future (): FutureInterface {

    let resolve = undefined
        , reject = undefined

    const p = new Promise ((resolve_, reject_) => {
        resolve = resolve_
        reject = reject_
    })

    // synchronous settlement subscribers (used by Future.race to avoid
    // paying an extra microtask hop per delivered message)
    // 0 = pending, 1 = fulfilled, 2 = rejected
    let settledState = 0
    let settledValue = undefined
    let handlers = []

    const notify = () => {
        const currentHandlers = handlers
        handlers = []
        for (let i = 0; i < currentHandlers.length; i++) {
            const handler = currentHandlers[i]
            if (settledState === 1) {
                handler.onFulfil (settledValue)
            } else {
                handler.onReject (settledValue)
            }
        }
    }

    p.resolve = function _resolve (value) {
        resolve (value)
        if (settledState === 0) {
            settledState = 1
            settledValue = value
            notify ()
        }
    }

    p.reject = function _reject (reason) {
        reject (reason)
        if (settledState === 0) {
            settledState = 2
            settledValue = reason
            notify ()
        }
    }

    // a future observed through subscribe() delivers its settlement through
    // the synchronous side-channel, not through the native promise - without
    // a native rejection handler an out-of-band reject() (e.g. client.reject
    // on exchange.close()) would crash the process as an unhandled rejection
    let nativeRejectionHandled = false

    // Registers synchronous settlement callbacks. Returns an unsubscribe
    // function that detaches the callbacks (mirrors the leak-safety that
    // Unpromise.race used to provide for repeatedly-raced futures).
    // If the future is already settled the matching callback fires
    // synchronously before subscribe() returns.
    p.subscribe = function _subscribe (onFulfil, onReject) {
        if (!nativeRejectionHandled) {
            nativeRejectionHandled = true
            p.catch (() => {})
        }
        if (settledState === 1) {
            onFulfil (settledValue)
            return () => {}
        }
        if (settledState === 2) {
            onReject (settledValue)
            return () => {}
        }
        const handler = { 'onFulfil': onFulfil, 'onReject': onReject }
        handlers.push (handler)
        return () => {
            const index = handlers.indexOf (handler)
            if (index >= 0) {
                handlers.splice (index, 1)
            }
        }
    }

    return p
}

const pendingPromiseFutures = new WeakMap<Promise<any>, FutureInterface> ()

function asFuture (value): FutureInterface {
    if (value && (typeof value.subscribe === 'function')) {
        return value
    }
    return asPromiseFuture (value)
}

function asPromiseFuture (value): FutureInterface {
    const promise = Promise.resolve (value)
    const cached = pendingPromiseFutures.get (promise)
    if (cached !== undefined) {
        return cached
    }
    const future = Future ()
    pendingPromiseFutures.set (promise, future)
    try {
        promise.then ((result) => {
            // Удаление до уведомления сохраняет асинхронность повторного наблюдения из callback.
            pendingPromiseFutures.delete (promise)
            future.resolve (result)
        }, (error) => {
            pendingPromiseFutures.delete (promise)
            future.reject (error)
        })
    } catch (error) {
        if (pendingPromiseFutures.get (promise) === future) {
            pendingPromiseFutures.delete (promise)
        }
        throw error
    }
    return future
}

function raceFutures (futures, toFuture): FutureInterface {
    const result = Future ()
    const unsubscribers = []
    let done = false
    const detach = () => {
        for (const unsubscribe of unsubscribers) {
            unsubscribe ()
        }
        unsubscribers.length = 0
    }
    const settleWith = (settler) => (value) => {
        if (done) {
            return
        }
        done = true
        detach ()
        settler (value)
    }
    const onFulfil = settleWith (result.resolve)
    const onReject = settleWith (result.reject)
    result.resolve = onFulfil
    result.reject = onReject
    try {
        for (const future of futures) {
            const unsubscribe = toFuture (future).subscribe (onFulfil, onReject)
            if (done) {
                unsubscribe ()
                break
            }
            unsubscribers.push (unsubscribe)
        }
    } catch (error) {
        done = true
        detach ()
        throw error
    }
    return result
}

Future.race = (futures): FutureInterface => raceFutures (futures, asFuture)

// Нативное состояние Promise учитывает принятие другого Promise, в отличие от синхронного канала Future.
Future.raceAsync = (futures): FutureInterface => raceFutures (futures, asPromiseFuture)
