import * as fs from 'fs-extra';
import { EventStream } from './event_stream';
import { LineStream, streamLinesAsync } from './line_stream';

export class StructuredQueryLog {
  public async readFile(fileLocation: string){
    const stream = fs.createReadStream(fileLocation);
    return streamLinesAsync(stream).thenNew(Parser).get();
  }
}

export interface LogStream {
  end: EventStream<void>;
}

class Parser implements LogStream {
  public readonly end: EventStream<void>;

  constructor(public readonly input: LineStream) {
    this.end = input.end;

    input.on(/CSV_IMB_QUERIES:\s*(.*)/, ([_, row]) => {
      console.log(row);
    });
  }
}