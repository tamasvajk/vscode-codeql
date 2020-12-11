import * as fs from 'fs-extra';
import { getDominanceRelation } from './dominators';
import { streamLinesAsync } from './line_stream';
import { abbreviateStrings } from './string_set_abbreviation';
import { getStronglyConnectedComponents, Scc } from './strongly_connected_components';
import { getDependenciesFromRA, StageEndedEvent, Parser, LogStream, PipelineEvaluationEvent } from './query-history';
import { getInverse, withoutNulls } from './util';

export async function readFile(fileLocation: string): Promise<FlameGraphNode> {
  const stream = fs.createReadStream(fileLocation);
  return streamLinesAsync(stream).thenNew(Parser).thenNew(FlamegraphBuilder).get().then(builder => builder.finish());
}

export interface FlameGraphNode {
  kind?: string;
  name: string;
  value: number;
  children: FlameGraphNode[];
  rawLines?: string[][];
  ownValue?: number;
}

type SccNode = Scc<string>;

class PredicateNode {
  constructor(readonly name: string) { }
  tupleCount = 0;
  dependencies = new Set<string>();
  dependents = new Set<string>();
  seenEvaluation = false;

  rawLines: string[][] = [];

  scc: SccNode | undefined;
}

export class FlamegraphBuilder {
  predicateNodes = new Map<string, PredicateNode>();
  stageNodes: FlameGraphNode[] = [];

  constructor(input: LogStream) {
    input.onPipeline.listen(this.onPipeline.bind(this));
    input.onStageEnded.listen(this.onStageEnded.bind(this));
  }

  private getPredicateNode(name: string) {
    let result = this.predicateNodes.get(name);
    if (result == null) {
      result = new PredicateNode(name);
      this.predicateNodes.set(name, result);
    }
    return result;
  }

  private onStageEnded(event: StageEndedEvent) {
    this.stageNodes.push(this.getFlamegraphNodeFromStage(event));
    this.predicateNodes.clear();
  }

  private onPipeline(pipeline: PipelineEvaluationEvent) {
    const name = pipeline.predicateName;
    const node = this.getPredicateNode(name);
    node.seenEvaluation = true;
    for (const step of pipeline.steps) {
      node.tupleCount += step.tupleCount;
      for (const otherRelation of getDependenciesFromRA(step.body).inputRelations) {
        node.dependencies.add(otherRelation);
        this.getPredicateNode(otherRelation).dependents.add(name);
      }
    }
    // node.rawLines.push(pipeline.rawLines);
  }

  private getRoots() {
    const roots: string[] = [];
    this.predicateNodes.forEach((data, name) => {
      if (data.dependents.size === 0) {
        roots.push(name);
      }
    });
    return roots;
  }

  private getFlamegraphNodeFromPredicate(predicate: string, dominated: Map<SccNode | null, SccNode[]>, successors: SccNode[]): FlameGraphNode | undefined {
    const node = this.getPredicateNode(predicate);
    if (!node.seenEvaluation) { return undefined; }
    const children: FlameGraphNode[] = [];
    for (const successor of successors) {
      const child = this.getFlamegraphNodeFromScc(successor, dominated);
      if (child != null) {
        children.push(child);
      }
    }
    const value = node.tupleCount + totalValue(children);
    return {
      name: node.name,
      value,
      ownValue: node.tupleCount,
      children,
      rawLines: node.rawLines,
    };
  }

  private getFlamegraphNodeFromScc(scc: SccNode, dominated: Map<SccNode | null, SccNode[]>): FlameGraphNode | undefined {
    const { members } = scc;
    if (members.length === 1) {
      return this.getFlamegraphNodeFromPredicate(members[0], dominated, dominated.get(scc) ?? []);
    }
    const name = abbreviateStrings(members);
    const children: FlameGraphNode[] = [];
    for (const member of members) {
      const child = this.getFlamegraphNodeFromPredicate(member, dominated, []);
      if (child != null) {
        children.push(child);
      }
    }
    const successors = dominated.get(scc) ?? [];
    successors.forEach(otherScc => {
      const child = this.getFlamegraphNodeFromScc(otherScc, dominated);
      if (child != null) {
        children.push(child);
      }
    });
    return {
      name,
      value: totalValue(children),
      children,
    };
  }

  private getFlamegraphNodeFromStage(stage: StageEndedEvent): FlameGraphNode {
    const roots = this.getRoots();
    const predicates = Array.from(this.predicateNodes.keys());
    const sccMap = getStronglyConnectedComponents(predicates, pred => this.getPredicateNode(pred).dependencies);
    sccMap.nodes.forEach((scc, predicate) => {
      this.getPredicateNode(predicate).scc = scc;
    });
    const rootSccs = roots.map(r => sccMap.nodes.get(r)!);
    const sccDominators = getDominanceRelation(rootSccs, scc => scc.successors);
    const sccDominated = getInverse(sccDominators);

    const levelOneNodes = withoutNulls(rootSccs.map(n => this.getFlamegraphNodeFromScc(n, sccDominated)));
    return {
      kind: 'Stage',
      name: abbreviateStrings(stage.queryPredicates),
      value: totalValue(levelOneNodes),
      children: levelOneNodes,
    };
  }

  finish(): FlameGraphNode {
    const children = this.stageNodes;
    return {
      name: 'root',
      value: totalValue(children),
      children: children,
    };
  }
}

function totalValue(children: FlameGraphNode[]) {
  return children.reduce((x, y) => x + y.value, 0);
}
