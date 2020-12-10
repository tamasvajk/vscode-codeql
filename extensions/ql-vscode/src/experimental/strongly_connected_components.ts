import { Collection } from './util';

export class Scc<T> {
  constructor(readonly index: number) { }

  readonly members: T[] = [];
  readonly successors: Set<Scc<T>> = new Set<Scc<T>>();
}

export interface SccMapping<T> {
  readonly nodes: Map<T, Scc<T>>;
  readonly sccs: Scc<T>[];
}

export function getStronglyConnectedComponents<Node>(roots: Collection<Node>, successors: (node: Node) => Collection<Node>): SccMapping<Node> {
  class NodeData {
    lowlink: number;
    scc: Scc<Node> | undefined;
    successorSccs: Scc<Node>[] = [];

    constructor(readonly node: Node, readonly index: number) {
      this.lowlink = index;
    }

    onStack() {
      return this.scc == null;
    }
  }

  let nextIndex = 0;
  const nodeDatas = new Map<Node, NodeData>();
  const stack: NodeData[] = [];

  const sccs: Scc<Node>[] = [];
  const sccNodes = new Map<Node, Scc<Node>>();

  function visit(node: Node): NodeData {
    let data = nodeDatas.get(node);
    if (data != null) { return data; }
    data = new NodeData(node, nextIndex++);
    nodeDatas.set(node, data);
    stack.push(data);

    successors(node).forEach((succ: Node) => {
      let succData = nodeDatas.get(succ);
      if (succData == null) {
        succData = visit(succ);
        data!.lowlink = Math.min(data!.lowlink, succData.lowlink);
      } else if (succData.onStack()) {
        data!.lowlink = Math.min(data!.lowlink, succData.index);
      }
      const succScc = succData.scc;
      if (succScc != null) {
        data!.successorSccs.push(succScc);
      }
    });

    if (data.lowlink === data.index) {
      const scc = new Scc<Node>(sccs.length);
      sccs.push(scc);
      let current: NodeData;
      do {
        current = stack.pop()!;
        current.scc = scc;
        sccNodes.set(current.node, scc);
        scc.members.push(current.node);
        for (const succScc of current.successorSccs) {
          scc.successors.add(succScc);
        }
      } while (current !== data);
    }

    return data;
  }

  roots.forEach(visit);

  return { nodes: sccNodes, sccs };
}
