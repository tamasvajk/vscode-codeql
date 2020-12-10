// do we need DIL predicates?

export interface LogFile {
  queries: Query[];
}

export interface Query {
  name: string;
  stages: Stage[];
  raPredicates?: RaPredicate[];    // dict: name -> predicate ?
  //results?: RaPredicate[];         // predicates after lines matching RESULTS IN:
  //dilPredicates?: DilPredicate[];  // dict: name -> predicate ?

  startLine?: SourceLine;          // match: Start query execution
  endLine?: SourceLine;            // match: CSV_IMB_QUERIES: Query,
}

export interface Stage {
  predicates: RaPredicate[];
  stageNumber: number;
  stageTime: number;
  numTuples: number;
  startLine?: SourceLine;      // match: [STAGING] {Evaluate|Do not evaluate} program stage for predicate(s) ...
  endLine?: SourceLine;        // match: CSV_IMB_QUERIES ...
}

export interface RaPredicate {
  name: string;
  //dilPredicate?: DilPredicate;
  evaluations: PipelineEvaluation[];
  // delta: number; ?
  // isExtensional: boolean; ?
  rowCount?: number;
  evaluationTime?: number;  // 999 from match: Xss.ql-4:AST::ASTNode::getTopLevel#ff ................... 999ms (executed 168 times)
  executionCount?: number;  // 168, does this match evaluations.length?
}

// // ??? do we need this?
// export interface DilPredicate {
//   name: string;
//   raPredicates: RaPredicate[];
//   lines: SourceLine[];
// }

export interface PipelineEvaluation {
  steps: PipelineStep[];
  lines: SourceLine[];
  // delta: number; // TODO iteration numbers for recursive delta predicates
}

// match: 963     ~0%     {1} r2 = JOIN r1 WITH stmts_10#join_rhs AS R ON FIRST 1 OUTPUT R.<1>
export interface PipelineStep {
  //evaluation?: PipelineEvaluation; // this should just be the parent, so hopefully not necessary
  body: string;                   // JOIN r1 WITH stmts_10#join_rhs AS R ON FIRST 1 OUTPUT R.<1>
  tupleCount: number;             // 963
  duplication: number;            // 0
  arity: number;                  // 1
  target: number;                 // 2 from r2

  subPredicates: RaPredicate[];   // [ stmts_10#join_rhs ]
  subRelations: number[];         // [ 1 ]

  line: SourceLine;
}

export interface SourceLine {
  text: string;
  lineNumber: number;
}
