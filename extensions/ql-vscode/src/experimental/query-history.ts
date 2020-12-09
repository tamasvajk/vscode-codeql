import * as fs from 'fs-extra';
import { LogFile } from './dataModel';
import { EventStream } from './event_stream';
import { LineStream, streamLinesAsync } from './line_stream';

export class StructuredQueryLog {
  public async readFile(fileLocation: string): Promise<LogFile> {
    const stream = fs.createReadStream(fileLocation);
    return streamLinesAsync(stream).thenNew(Parser).get().then(p => p.getLogFile());
  }
}

export interface LogStream {
  end: EventStream<void>;
}

class Parser implements LogStream {
  private logFile: LogFile;

  getLogFile(): LogFile {
    return this.logFile;
  }
  public readonly end: EventStream<void>;

  constructor(public readonly input: LineStream) {
    this.end = input.end;

    this.logFile = { queries: [] };

    input.on(/CSV_IMB_QUERIES:\s*(.*)/, ([_, row]) => {
      console.log(row);
    });
  }
}
