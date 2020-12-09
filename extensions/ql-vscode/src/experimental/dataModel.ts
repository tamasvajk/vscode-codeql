// do we need DIL predicates?

export interface LogFile {
  queries: Query[];
}

export interface Query {
  stages: Stage[];
  results: RaPredicate[];
  raPredicates: RaPredicate[];    // dict: name -> predicate ?
  dilPredicates: DilPredicate[];  // dict: name -> predicate ?

  startLine: SourceLine;          // match: Start query execution
  endLine: SourceLine;            // match: CSV_IMB_QUERIES: Query,
}

export interface Stage {
  predicates: RaPredicate[];
  startLine: SourceLine;      // match: [STAGING] Evaluate program stage for predicate(s) ...
  endLine: SourceLine;        // match: CSV_IMB_QUERIES ...
}

export interface RaPredicate {
  name: string;
  dilPredicate?: DilPredicate;
  evaluations: Evaluation[];
  // delta: number; ?
  // isExtensional: boolean; ?
  rowCount: number;
  evaluationTime: number;  // 999 from match: Xss.ql-4:AST::ASTNode::getTopLevel#ff ................... 999ms (executed 168 times)
  executionCount: number;  // 168, does this match evaluations.length?
}

// ??? do we need this?
export interface DilPredicate {
  name: string;
  raPredicates: RaPredicate[];
  lines: SourceLine[];
}

export interface Evaluation {
  predicate: RaPredicate;
  steps: PipelineStep[];
  lines: SourceLine[];
  // delta: number; ?
}

// match: 963     ~0%     {1} r2 = JOIN r1 WITH stmts_10#join_rhs AS R ON FIRST 1 OUTPUT R.<1>
export interface PipelineStep {
  evaluation: Evaluation;
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
