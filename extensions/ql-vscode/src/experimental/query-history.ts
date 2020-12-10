import * as fs from 'fs-extra';
import { LogFile, PipelineStep, Query, RaPredicate, SourceLine, Stage } from './dataModel';
import { EventStream } from './event_stream';
import { LineStream, MatchEvent, streamLinesAsync } from './line_stream';

export class StructuredQueryLog {
  public async readFile(fileLocation: string): Promise<LogFile> {
    const stream = fs.createReadStream(fileLocation);
    return streamLinesAsync(stream).thenNew(Parser).thenNew(StructuredLogBuilder).get().then(builder => builder.getLogFile());
  }
}

class StructuredLogBuilder {
  private queries: Query[] = [];
  private queryToStages: Map<string, Stage[]> = new Map<string, Stage[]>();
  private predicates: Map<string, RaPredicate> = new Map<string, RaPredicate>();
  constructor(input: Parser) {
    input.onPipeline.listen(this.onPipeline.bind(this));
    input.onPredicateCompletion.listen(this.onPredicateSize.bind(this));
    input.onPredicateExecutionTime.listen(this.onPredicateExecutionTime.bind(this));
    input.onStageEnded.listen(this.onStageEnded.bind(this));
  }

  private onPipeline(event: PipelineEvaluationEvent) {
    if (!this.predicates.has(event.predicateName)) {
      this.predicates.set(event.predicateName, { name: event.predicateName, evaluations: [] });
    }
    this.predicates.get(event.predicateName)!.evaluations.push(event);
  }

  private onPredicateExecutionTime(event: PredicateExecTimeEvent) {
    if (!this.predicates.has(event.name)) {
      this.predicates.set(event.name, { name: event.name, evaluations: [] });
    }
    this.predicates.get(event.name)!.evaluationTime = event.executionTime;
  }

  private onPredicateSize(event: PredicateSizeEvent) {
    if (!this.predicates.has(event.name)) {
      this.predicates.set(event.name, { name: event.name, evaluations: [] });
    }
    // Only update if we receive a tuple count.
    // We don't want a later cache hit line (without counts) to overwrite
    // tuple counts that were previously recorded.
    if (event.numTuples !== undefined) {
      this.predicates.get(event.name)!.rowCount = event.numTuples;
    }
  }

  private onStageEnded(stageNode: StageEndedEvent) {
    if (!this.queryToStages.has(stageNode.queryName)) {
      this.queryToStages.set(stageNode.queryName, []);
    }
    const stage: Stage = {
      stageNumber: stageNode.stageNumber,
      stageTime: stageNode.stageTime,
      numTuples: stageNode.numTuples,
      predicates: stageNode.queryPredicates.map(name => {
        var pred = this.predicates.get(name);
        if (!pred) {
          console.error(`Couldn't find predicate ${name}`);
          pred = { name, evaluations: [] };
        }
        return pred;
      }),
      startLine: stageNode.startLine,
      endLine: stageNode.endLine
    };
    this.queryToStages.get(stageNode.queryName)!.push(stage);

    if (stageNode.isQueryEnd) {
      var predicates = new Map(this.predicates);
      this.predicates.clear();

      this.matchSubPredicates(predicates);

      // query ended.
      const query: Query = {
        name: stageNode.queryName,
        startLine: stageNode.queryStartLine,
        endLine: stageNode.endLine,
        stages: this.queryToStages.get(stageNode.queryName)!.sort((s1, s2) => s1.stageNumber - s2.stageNumber),
        raPredicates: [...predicates.values()]
      };

      this.queries.push(query);
    }
  }

  matchSubPredicates(predicates: Map<string, RaPredicate>) {
    for (const predicate of predicates.values()) {
      for (const evaluation of predicate.evaluations) {
        for (const step of evaluation.steps) {
          const dependency = getDependenciesFromRA(step.body);
          step.subRelations = dependency.inputVariables;
          step.subPredicates = dependency.inputRelations
            .map(r => predicates.get(r))
            .filter(x => x != null) as NonNullable<RaPredicate>[];
        }
      }
    }
  }

  getLogFile(): LogFile {
    return {
      queries: this.queries
    };
  }
}

export interface LogStream {
  onPipeline: EventStream<PipelineEvaluationEvent>;
  onPredicateCompletion: EventStream<PredicateSizeEvent>;
  onStageEnded: EventStream<StageEndedEvent>;
  end: EventStream<void>;
}

// TODO this could just be a Stage
export interface StageEndedEvent {
  queryPredicates: string[];
  queryName: string;
  stageNumber: number;
  stageTime: number;
  numTuples: number;
  startLine?: SourceLine;
  endLine: SourceLine;
  isQueryEnd: boolean;
  queryStartLine?: SourceLine;
}

export interface QueryStartEvent {
  startLine: SourceLine;
}

interface PredicateSizeEvent {
  name: string;
  numTuples?: number;
}

interface PredicateExecTimeEvent {
  name: string;
  executionTime: number;
}

export interface PipelineEvaluationEvent {
  predicateName: string;
  steps: PipelineStep[];
  lines: SourceLine[];
  // delta: number; // TODO iteration numbers for recursive delta predicates
}

export class Parser implements LogStream {
  public readonly onQueryStarting = new EventStream<QueryStartEvent>();
  public readonly onQueryEnded = new EventStream<void>();
  public readonly onPipeline = new EventStream<PipelineEvaluationEvent>();
  public readonly onPredicateCompletion = new EventStream<PredicateSizeEvent>();
  public readonly onPredicateExecutionTime = new EventStream<PredicateExecTimeEvent>();
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

    let stageStartLine: SourceLine | undefined = undefined;
    let queryStartLine: SourceLine | undefined = undefined;

    input.on(/\s*Start query execution/, ({ match, lineNumber }) => {
      const [wholeLine,] = match;
      console.log('Found query starting on line ' + lineNumber);
      queryStartLine = { text: wholeLine, lineNumber: lineNumber || 0 };
    });

    // Start of a stage
    input.on(/\s*\[STAGING\]\s*.*/, ({ match, lineNumber }) => {
      const [wholeLine,] = match;
      console.log('Found stage starting on line ' + lineNumber);
      stageStartLine = { text: wholeLine, lineNumber: lineNumber || 0 };
    });

    // End of a stage (or query, but ignore those for now)
    input.on(/CSV_IMB_QUERIES:\s*(.*)/, ({ match, lineNumber }) => {
      const [wholeLine, row] = match;
      // The first occurrence is usually the header, but not always, so matching on the expected header instead:
      if (row === 'Query type,Query predicate,Query name,Stage,Success,Time,Number of results,Cumulative time in query') {
        return;
      }

      // Process the row data
      const rowEntries = row.split(',');
      const [entryType, queryPredicates, queryName, stageNumber, , stageTime, numTuples,] = rowEntries;
      if (entryType.toLowerCase() === 'query') {
        // TODO we don't have an accurate start line for the final stage
        console.log(`Found final stage and query completion on line ${lineNumber}`, row);
      } else {
        console.log(`Found stage completion on line ${lineNumber} ${stageStartLine ? 'with' : 'without'} a start line`, row);
      }
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
        endLine,
        isQueryEnd: entryType.toLowerCase() === 'query',
        queryStartLine: queryStartLine
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
      currentPredicateName = rewritePredicateName(name);
      currentPredicateLine = lineNumber;
      // currentRawLines.push(match.input!);
    });

    // Start of tuple counts for a predicate. Similar to the above case, with trailing colon.
    // Tuple counts for name/arity@hash:
    input.on(/Tuple counts for (.*)\/(\d+).*:/, ({ match, lineNumber }) => {
      const [, name, arity] = match;
      console.log(`Saw tuple count logs for ${name} with arity ${arity}`);
      this.seenPredicateEvaluation = true;
      currentPredicateName = rewritePredicateName(name);
      currentPredicateLine = lineNumber;
      // currentRawLines.push(match.input!);
    });

    // Tuple count lines for an entire predicate upon completion.
    const parseRelationSize = (input: MatchEvent) => {
      const [, name, numTuples] = input.match;
      console.log(`Saw complete relation ${name} with ${numTuples} tuples`);
      this.onPredicateCompletion.fire({
        name: rewritePredicateName(name),
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
        name: rewritePredicateName(name)
      });
    };
    // Cache hit log lines don't have the tuple counts anymore.
    input.on(/Found relation ([\w#:]+)\/(?:\d+)@\w+/, parseRelationWithoutSize);
    input.on(/Cache hit for relation ([\w#:]+)\/(?:\d+)@\w+/, parseRelationWithoutSize);

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
    });

    input.on(/^\t([A-Za-z.0-9]+)-(\d+):(.*) \.+ (?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m)?(?:(\d+(?:\.\d+)?)s)?(?:(\d+(?:\.\d+)?)ms)?\b.*$/, ({ match }) => {
      const [, query, stageStr, name, hStr, mStr, sStr, msStr] = match;
      console.log(`Saw relation ${name} execution time`);
      this.onPredicateExecutionTime.fire({
        name: rewritePredicateName(name),
        executionTime: this.getExecMs(hStr, mStr, sStr, msStr)
      });
    });
  }

  getExecMs(hStr: string, mStr: string, sStr: string, msStr: string): number {
    let val = 0;
    if (hStr) {
      val += 1000 * 60 * 60 * Number(hStr);
    }
    if (mStr) {
      val += 1000 * 60 * Number(mStr);
    }
    if (sStr) {
      val += 1000 * Number(sStr);
    }
    if (msStr) {
      val += Number(msStr);
    }
    return val;
  }
}

export interface RADependencies {
  inputVariables: number[];
  inputRelations: string[];
}

function allMatches(regexp: RegExp, input: string): RegExpMatchArray[] {
  if (!regexp.flags.includes('g')) { throw new Error('allMatches requires a RegExp with /g flag'); }
  let match: RegExpMatchArray | null;
  const result = [];
  while ((match = regexp.exec(input)) != null) {
    result.push(match);
  }
  return result;
}

export function getDependenciesFromRA(racode: string): RADependencies {
  const inputVariables = new Set<number>();
  const inputRelations = new Set<string>();
  const stripped = racode.replace(/"[^"]+"/g, '""');
  for (const [ref] of allMatches(/(?<!HIGHER-ORDER RELATION |PRIMITIVE |[$@#])\b[a-zA-Z#][\w:#_]+\b(?!\()/g, stripped)) {
    if (/^([A-Z]+|true|false)$/.test(ref)) { continue; } // Probably an RA keyword
    if (/^r\d+$/.test(ref)) {
      inputVariables.add(Number(ref.substring(1)));
    } else {
      inputRelations.add(rewritePredicateName(ref));
    }
  }
  return {
    inputVariables: Array.from(inputVariables),
    inputRelations: Array.from(inputRelations)
  };
}

export function rewritePredicateName(name: string): string {
  return name.replace(/#(cur_delta|prev_delta|prev)/, ''); // todo: do we need to remove these endings too: "@staged_ext", "_delta"?
}
