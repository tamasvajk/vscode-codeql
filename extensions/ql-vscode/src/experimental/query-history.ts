import * as fs from 'fs-extra';
import { LogFile, PipelineEvaluation, PipelineStep, Query, SourceLine, Stage } from './dataModel';
import { EventStream } from './event_stream';
import { LineStream, MatchEvent, streamLinesAsync } from './line_stream';

export class StructuredQueryLog {
  public async readFile(fileLocation: string): Promise<LogFile> {
    const stream = fs.createReadStream(fileLocation);
    return streamLinesAsync(stream).thenNew(Parser).thenNew(StructuredLogBuilder).get().then(builder => builder.getLogFile());
  }
}

class StructuredLogBuilder {
  private stageNodes: StageEndedEvent[] = [];
  private predicateSizes: Map<string, number | undefined> = new Map<string, number | undefined>();
  private predicateEvaluations: Map<string, PipelineEvaluation[]> = new Map<string, PipelineEvaluation[]>();
  constructor(input: LogStream) {
    input.onPipeline.listen(this.onPipeline.bind(this));
    input.onPredicateCompletion.listen(this.onPredicateSize.bind(this));
    input.onStageEnded.listen(this.onStageEnded.bind(this));
  }

  private onPipeline(event: PipelineEvaluation) {
    if (!this.predicateEvaluations.has(event.predicateName)) {
      this.predicateEvaluations.set(event.predicateName, []);
    }
    this.predicateEvaluations.get(event.predicateName)!.push(event);
  }

  private onPredicateSize(event: PredicateSizeEvent) {
    // Only update if we receive a tuple count.
    // We don't want a later cache hit line (without counts) to overwrite
    // tuple counts that were previously recorded.
    if (event.numTuples !== undefined) {
      this.predicateSizes.set(event.name, event.numTuples);
    }
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
        predicates: stageNode.queryPredicates.map(name => ({
          name,
          rowCount: this.predicateSizes.get(name),
          // TODO this assumes all the evaluations are from the same stage. I hope that is true.
          evaluations: this.predicateEvaluations.get(name) || []
        })),
        startLine: stageNode.startLine,
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
  onPipeline: EventStream<PipelineEvaluation>;
  onPredicateCompletion: EventStream<PredicateSizeEvent>;
  onStageEnded: EventStream<StageEndedEvent>;
  end: EventStream<void>;
}

// TODO this could just be a Stage
interface StageEndedEvent {
  queryPredicates: string[];
  queryName: string;
  stageNumber: number;
  stageTime: number;
  numTuples: number;
  startLine?: SourceLine;
  endLine: SourceLine;
}

interface PredicateSizeEvent {
  name: string;
  numTuples?: number;
}

class Parser implements LogStream {
  public readonly onPipeline = new EventStream<PipelineEvaluation>();
  public readonly onPredicateCompletion = new EventStream<PredicateSizeEvent>();
  public readonly onStageEnded = new EventStream<StageEndedEvent>();
  public readonly end: EventStream<void>;

  /**
   * Set to true if the evaluation of a predicate was seen in the
   * parsed log.
   *
   * Can be used to diagnose cases where no tuple counts were found,
   * indicating if this was a log without tuple counts, or not a log
   * file at all.
   */
  public seenPredicateEvaluation = false;

  constructor(public readonly input: LineStream) {
    this.end = input.end;

    let seenCsvImbQueriesHeader = false;
    let stageStartLine: SourceLine | undefined = undefined;

    // Start of a stage
    input.on(/\s*\[STAGING\]\s*.*/, ({ match, lineNumber }) => {
      const [wholeLine,] = match;
      console.log('Found stage starting on line ' + lineNumber);
      stageStartLine = { text: wholeLine, lineNumber: lineNumber || 0 };
    });

    // End of a stage (or query, but ignore those for now)
    input.on(/CSV_IMB_QUERIES:\s*(.*)/, ({ match, lineNumber }) => {
      const [wholeLine, row] = match;
      // The first occurrence is the header
      // Query type,Query predicate,Query name,Stage,Success,Time,Number of results,Cumulative time in query
      if (!seenCsvImbQueriesHeader) {
        seenCsvImbQueriesHeader = true;
        return;
      }

      // Process the row data
      const rowEntries = row.split(',');
      const [entryType, queryPredicates, queryName, stageNumber, , stageTime, numTuples,] = rowEntries;
      if (entryType.toLowerCase() === 'query') {
        console.log(`Found query completion on line ${lineNumber}`, row);
        return;
      }
      console.log(`Found stage completion on line ${lineNumber} ${stageStartLine ? 'with' : 'without'} a start line`, row);
      const startLine = stageStartLine;
      stageStartLine = undefined;

      const endLine: SourceLine = { text: wholeLine, lineNumber: lineNumber || 0 };
      this.onStageEnded.fire({
        queryName,
        queryPredicates: queryPredicates.split(' '),
        stageNumber: Number.parseInt(stageNumber),
        stageTime: Number.parseFloat(stageTime),
        numTuples: Number.parseInt(numTuples),
        startLine,
        endLine
      });
    });

    let currentPredicateName: string | undefined = undefined;
    let currentPredicateLine: number | undefined = undefined;
    console.log(currentPredicateLine); // TODO include line number in output
    let currentPipelineSteps: PipelineStep[] = [];

    // Start of a predicate
    // Starting to evaluate predicate name/arity@hash
    input.on(/Starting to evaluate predicate (.*)\/(\d+).*/, ({ match, lineNumber }) => {
      const [, name, arity] = match;
      console.log(`Saw Starting to evaluate for ${name} with arity ${arity}`);
      this.seenPredicateEvaluation = true;
      currentPredicateName = name;
      currentPredicateLine = lineNumber;
      // currentRawLines.push(match.input!);
    });

    // Start of tuple counts for a predicate. Similar to the above case, with trailing colon.
    // Tuple counts for name/arity@hash:
    input.on(/Tuple counts for (.*)\/(\d+).*:/, ({ match, lineNumber }) => {
      const [, name, arity] = match;
      console.log(`Saw tuple count logs for ${name} with arity ${arity}`);
      this.seenPredicateEvaluation = true;
      currentPredicateName = name;
      currentPredicateLine = lineNumber;
      // currentRawLines.push(match.input!);
    });

    // Tuple count lines for an entire predicate upon completion.
    const parseRelationSize = (input: MatchEvent) => {
      const [, name, numTuples] = input.match;
      console.log(`Saw complete relation ${name} with ${numTuples} tuples`);
      this.onPredicateCompletion.fire({
        name,
        numTuples: Number(numTuples),
      });
    };
    // Some log lines have the tuple counts.
    const predicateTupleCountRegexes = [
      />>> Relation ([\w#:]+): (\d+) rows/,
      />>> (?:Wrote|Created) relation ([\w#:]+)\/(?:\d+)@\w+ with (\d+) rows/,
      /- ([\w#:]+) has (\d+) rows/,
      /Found relation ([\w#:]+)\/(?:\d+)@\w+\b.*\bRelation has (\d+) rows/
    ];
    for (const regex of predicateTupleCountRegexes) {
      input.on(regex, parseRelationSize);
    }
    const parseRelationWithoutSize = (input: MatchEvent) => {
      const [, name] = input.match;
      console.log(`Saw complete relation ${name} without a final tuple count`);
      this.onPredicateCompletion.fire({
        name
      });
    };
    // Cache hit log lines don't have the tuple counts anymore.
    input.on(/Found relation ([\w#:]+)\/(?:\d+)@\w+/, parseRelationWithoutSize);

    // Tuple counts for each step within a pipeline
    // 963     ~0%     {1} r2 = JOIN r1 WITH stmts_10#join_rhs AS R ON FIRST 1 OUTPUT R.<1> 'stmt'
    input.on(/(\d+)\s+(?:~(\d+)%)?\s+[{](\d+)[}]\s+r(\d+)\s+=\s+(.*)/, ({ match, lineNumber }) => {
      const [, tupleCountStr, duplicationStr, arityStr, resultVariableStr, body] = match;
      const tupleCount = Number(tupleCountStr);
      const duplication = Number(duplicationStr);
      const arity = Number(arityStr);
      const resultVariable = Number(resultVariableStr);
      console.log(`Saw pipeline step ${body} with ${tupleCount} tuples`);
      currentPipelineSteps.push({
        tupleCount,
        duplication,
        arity,
        body,
        line: { text: match.input!, lineNumber: lineNumber || 0 },
        subPredicates: [], // TODO
        subRelations: [], // TODO
        target: resultVariable
      });
      // currentRawLines.push(match.input!);
    }, () => {// Called if there was no match
      // Complete the current pipeline if there is one.
      if (currentPipelineSteps.length > 0 && currentPredicateName != null) {
        this.onPipeline.fire({
          predicateName: currentPredicateName,
          steps: currentPipelineSteps,
          lines: [] // TODO
          //startLine: {currentPredicateLine,
          // endLine: input.lineNumber,
          // rawLines: currentRawLines,
        });
        currentPipelineSteps = [];
        // currentRawLines = [];
        currentPredicateName = undefined;
      }
    }
    );
  }
}
