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
    ++this.lineNumber;
    for (const matcher of this.matchers) {
      const match = matcher.pattern.exec(line);
      if (match != null) {
        matcher.callback({ match, lineNumber: this.lineNumber });
      } else {
        const { negativeCallback } = matcher;
        if (negativeCallback != null) {
          negativeCallback({ line, lineNumber: this.lineNumber });
        }
      }
    }
  }

  addLines(lines: string[]): this {
    for (const line of lines) {
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
  on(pattern: RegExp, callback: Listener<MatchEvent>, negativeCallback?: Listener<NegativeMatchEvent>): this {
    this.matchers.push({ pattern, callback, negativeCallback });
    return this;
  }
}

interface MatchEvent {
  match: RegExpMatchArray;
  lineNumber?: number;
}

interface NegativeMatchEvent {
  line: string;
  lineNumber?: number;
}

interface Matcher {
  pattern: RegExp;
  callback: Listener<MatchEvent>;
  negativeCallback?: Listener<NegativeMatchEvent>;
}

/** Creates a `LineStream` and feeds it the given text once listeners have been added. */
export function streamLinesSync(text: string) {
  const parser = new LineStream();
  return new SyncStreamBuilder(() => parser.addText(text), parser);
}

/** Creates a `LineStream` from the given NodeJS stream. */
export function streamLinesAsync(stream: NodeJS.ReadableStream) {
  const parser = new LineStream();
  const readline = require('readline') as typeof import('readline');
  const reader = readline.createInterface(stream);
  reader.on('line', line => {
    parser.addLine(line);
  });
  reader.on('close', () => {
    parser.addEof();
  });
  return new AsyncStreamBuilder(parser.end, parser);
}
