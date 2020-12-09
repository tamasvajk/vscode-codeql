import { EventStream, Listener, SyncStreamBuilder, AsyncStreamBuilder } from './event_stream';

/**
 * Reads data line by line and invokes event handlers with associated regexps.
 */
export class LineStream {
    private matchers: Matcher[] = [];

    /** Event fired when there are no more lines to parse. */
    public readonly end = new EventStream<void>();

    /** Number of lines seen so far. */
    public lineNumber = 0;

    /** Adds a line and immediately invokes all matching event handlers. */
    addLine(line: string) {
        for (let matcher of this.matchers) {
            let match = matcher.pattern.exec(line);
            if (match != null) {
                matcher.callback(match);
            } else {
                let { negativeCallback } = matcher;
                if (negativeCallback != null) {
                    negativeCallback(line);
                }
            }
        }
        ++this.lineNumber;
    }

    addLines(lines: string[]): this {
        for (let line of lines) {
            this.addLine(line);
        }
        return this;
    }

    /** Marks the end of the file, firing the `end` event. */
    addEof() {
        this.end.fire();
    }

    /** Splits a text and invokes `addLine` for each line. */
    addText(text: string) {
        this.addLines(text.split(/\r?\n/));
        this.addEof();
    }

    /**
     * Listens for lines matching `pattern` and invokes `callback` on a match,
     * and `negativeCallback` (if provided) for any line that does not match.
     */
    on(pattern: RegExp, callback: Listener<RegExpMatchArray>, negativeCallback?: Listener<string>): this {
        this.matchers.push({pattern, callback, negativeCallback});
        return this;
    }
}

interface Matcher {
    pattern: RegExp;
    callback: Listener<RegExpMatchArray>;
    negativeCallback: undefined | Listener<string>;
}

/** Creates a `LineStream` and feeds it the given text once listeners have been added. */
export function streamLinesSync(text: string) {
    let parser = new LineStream();
    return new SyncStreamBuilder(() => parser.addText(text), parser);
}

/** Creates a `LineStream` from the given NodeJS stream. */
export function streamLinesAsync(stream: NodeJS.ReadableStream) {
    let parser = new LineStream();
    let readline = require('readline') as typeof import('readline');
    let reader = readline.createInterface(stream);
    reader.on('line', line => {
        parser.addLine(line);
    });
    reader.on('close', () => {
        parser.addEof();
    });
    return new AsyncStreamBuilder(parser.end, parser);
}
