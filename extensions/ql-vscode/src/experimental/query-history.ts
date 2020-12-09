import * as fs from 'fs-extra';
import { LogFile, Query, SourceLine, Stage } from './dataModel';
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
        stageTime: stageNode.stageTime,
        numTuples: stageNode.numTuples,
        predicates: stageNode.queryPredicates.map(name => ({ name, evaluations: [] })),
        endLine: stageNode.endLine
      };
      queryToStages.get(stageNode.queryName)!.push(stage);
    }
    const queries: Query[] = [];
    for (const [queryName, stages] of queryToStages.entries()) {
      // Stages are likely to be in order, but sort them to be consistent.
      queries.push({ name: queryName, stages: stages.sort((s1, s2) => s1.stageNumber - s2.stageNumber) });
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
  stageTime: number;
  numTuples: number;
  startLine?: SourceLine;
  endLine: SourceLine;
}

class Parser implements LogStream {
  public readonly onStageEnded = new EventStream<StageEndedEvent>();
  public readonly end: EventStream<void>;

  constructor(public readonly input: LineStream) {
    this.end = input.end;

    let seenCsvImbQueriesHeader = false;
    input.on(/CSV_IMB_QUERIES:\s*(.*)/, (matchEvent) => {
      const [wholeLine, row] = matchEvent.match;
      console.log(row);
      // The first occurrence is the header
      // Query type,Query predicate,Query name,Stage,Success,Time,Number of results,Cumulative time in query
      if (!seenCsvImbQueriesHeader) {
        seenCsvImbQueriesHeader = true;
        return;
      }
      // Process the row data
      const rowEntries = row.split(',');
      const [, queryPredicates, queryName, stageNumber, , stageTime, numTuples,] = rowEntries;
      const endLine: SourceLine = { text: wholeLine, lineNumber: matchEvent.lineNumber || 0 };
      this.onStageEnded.fire({
        queryName,
        queryPredicates: queryPredicates.split(' '),
        stageNumber: Number.parseInt(stageNumber),
        stageTime: Number.parseFloat(stageTime),
        numTuples: Number.parseInt(numTuples),
        endLine
      });
    });
  }
}
