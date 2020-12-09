import * as fs from 'fs-extra';
import { LogFile, Query, Stage } from './dataModel';
import { EventStream } from './event_stream';
import { LineStream, streamLinesAsync } from './line_stream';

export class StructuredQueryLog {
  public async readFile(fileLocation: string): Promise<LogFile> {
    const stream = fs.createReadStream(fileLocation);
    return streamLinesAsync(stream).thenNew(Parser).thenNew(StructuredLogBuilder).get().then(builder => builder.getLogFile());
  }
}

class StructuredLogBuilder {
  private stageNodes: StageEndedEvent[] = [];
  constructor(input: LogStream) {
    input.onStageEnded.listen(this.onStageEnded.bind(this));
  }

  private onStageEnded(event: StageEndedEvent) {
    this.stageNodes.push(event);
  }

  getLogFile(): LogFile {
    const queryToStages: Map<string, Stage[]> = new Map<string, Stage[]>();
    for (const stageNode of this.stageNodes) {
      if (!queryToStages.has(stageNode.queryName)) {
        queryToStages.set(stageNode.queryName, []);
      }
      const stage: Stage = {
        stageNumber: stageNode.stageNumber,
        predicates: stageNode.queryPredicates.map(name => ({ name, evaluations: [] }))
      };
      queryToStages.get(stageNode.queryName)!.push(stage);
    }
    const queries: Query[] = [];
    for (const [queryName, stages] of queryToStages.entries()) {
      queries.push({ name: queryName, stages });
    }
    return { queries };
  }
}

export interface LogStream {
  onStageEnded: EventStream<StageEndedEvent>;
  end: EventStream<void>;
}

interface StageEndedEvent {
  queryPredicates: string[];
  queryName: string;
  stageNumber: number;
}

class Parser implements LogStream {
  public readonly onStageEnded = new EventStream<StageEndedEvent>();
  public readonly end: EventStream<void>;

  constructor(public readonly input: LineStream) {
    this.end = input.end;

    let seenCsvImbQueriesHeader = false;
    input.on(/CSV_IMB_QUERIES:\s*(.*)/, ([_, row]) => {
      console.log(row);
      // The first occurrence is the header
      // Query type,Query predicate,Query name,Stage,Success,Time,Number of results,Cumulative time in query
      if (!seenCsvImbQueriesHeader) {
        seenCsvImbQueriesHeader = true;
        return;
      }
      // Process the row data
      const rowEntries = row.split(',');
      const [queryPredicates, queryName, stageNumber] = [rowEntries[1].split(' '), rowEntries[2], Number.parseInt(rowEntries[3])];
      this.onStageEnded.fire({
        queryName,
        queryPredicates,
        stageNumber
      });
    });
  }
}
