/**
 * An event listener for events of type `T`.
 */
export type Listener<T> = (event: T) => void;

/**
 * A list of event handlers for a given type of event.
 *
 * An object that exposes an event stream in a field will typically fire
 * its events, while users of the object can listen to the stream.
 */
export class EventStream<T> {
    public listeners: Listener<T>[] = [];

    /** Calls all listeners with the given event. */
    public fire(event: T) {
        for (let listener of this.listeners) {
            listener(event);
        }
    }

    /** Adds an event listener to be invoked when an event is fired. */
    public listen(listener: Listener<T>) {
        this.listeners.push(listener);
    }
}

/**
 * Helper for composing long streams before firing off the input events synchronously.
 *
 * Correct use of streams requires that all listeners have been added before any
 * events are fired. Otherwise the events are lost since we don't buffer the events.
 *
 * The stream builder knows how to trigger the input events synchronously, and encapsulates a value of type
 * `T` which is considered "ready" once all input events have fired.
 * The `.get` method triggers all input events and then returns the underlying value.
 */
export class SyncStreamBuilder<T> {
    constructor(
        private readonly fire: () => void,
        private readonly value: T) {}

    /**
     * Adds a consumer of the current stream value and returns the result boxed in a stream builder.
     */
    then<R>(transformer: (t: T) => R): SyncStreamBuilder<R> {
        return new SyncStreamBuilder(this.fire, transformer(this.value));
    }

    /**
     * Adds a consumer of the current stream value and returns the result boxed in a stream builder.
     */
    thenNew<R>(transformer: new (t: T) => R): SyncStreamBuilder<R> {
        return new SyncStreamBuilder(this.fire, new transformer(this.value));
    }

    /** Fires all input events and then returns the boxed value. */
    get(): T {
        this.fire();
        return this.value;
    }
}

/**
 * Helper for composing long streams and then waiting for the input stream to end.
 *
 * Correct use of streams requires that all the listeners have been added before any
 * events are fired. Otherwise the events are lost since we don't buffer the events.
 *
 * The stream builder knows when the input stream has ended, and encapsulates a value of type `T`
 * which is considered "ready" once all inputs events have fired.
 * The `.get` returns a promise for the underlying value when it is ready.
 */
export class AsyncStreamBuilder<T> {
    constructor(
        private readonly end: EventStream<any>,
        private readonly value: T) {}

    /**
     * Adds a consumer of the current stream value and returns the result boxed in a stream builder.
     */
    then<R>(transformer: (t: T) => R): AsyncStreamBuilder<R> {
        return new AsyncStreamBuilder(this.end, transformer(this.value));
    }

    /**
     * Adds a consumer of the current stream value and returns the result boxed in a stream builder.
     */
    thenNew<R>(transformer: new (t: T) => R): AsyncStreamBuilder<R> {
        return new AsyncStreamBuilder(this.end, new transformer(this.value));
    }

    /** Waits for the input stream to end, then returns the boxed value. */
    get(): Promise<T> {
        return new Promise<T>(resolve => {
            this.end.listen(() => {
                resolve(this.value);
            });
        });
    }
}
