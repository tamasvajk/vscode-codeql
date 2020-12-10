import { Collection } from './util';

export function getDominanceRelation<Node>(roots: Collection<Node>, successors: (node: Node) => Collection<Node>): Map<Node, Node | null> {
  class NodeData {
    constructor(readonly node: Node | null) { }

    readonly predecessors = new Set<Node>();
    postOrderId: number | undefined;
    dominator: NodeData | undefined;
  }
  const syntheticRoot = new NodeData(null);

  const nodeData = new Map<Node, NodeData>();
  const postOrderNodes: NodeData[] = [];

  function getNodeData(node: Node) {
    let data = nodeData.get(node);
    if (data == null) {
      data = new NodeData(node);
      nodeData.set(node, data);
    }
    return data;
  }

  function visitPostOrder(node: Node, pred: Node | undefined) {
    const data = getNodeData(node);
    if (pred != null) {
      data.predecessors.add(pred);
    }
    if (data.postOrderId != null) {
      return;
    }
    data.postOrderId = -1; // break cycles
    successors(node).forEach((succ: Node) => visitPostOrder(succ, node));
    data.postOrderId = postOrderNodes.length;
    postOrderNodes.push(data);
  }
  roots.forEach((root: Node) => visitPostOrder(root, undefined));

  syntheticRoot.postOrderId = postOrderNodes.length;

  roots.forEach((root: Node) => {
    const data = getNodeData(root);
    data.dominator = syntheticRoot;
  });

  function intersect(b1: NodeData, b2: NodeData): NodeData {
    while (b1 !== b2) {
      while (b1.postOrderId! < b2.postOrderId!) {
        b1 = b1.dominator!;
      }
      while (b2.postOrderId! < b1.postOrderId!) {
        b2 = b2.dominator!;
      }
    }
    return b1;
  }

  let changed = true;
  while (changed) {
    changed = false;
    for (let i = postOrderNodes.length - 1; i >= 0; --i) {
      const data = postOrderNodes[i];
      const oldDominator = data.dominator;
      data.predecessors.forEach(pred => {
        const predData = getNodeData(pred);
        if (predData.dominator === undefined) { return; }
        if (data.dominator === undefined) {
          data.dominator = predData;
        } else {
          data.dominator = intersect(data.dominator, predData);
        }
      });
      if (oldDominator !== data.dominator) {
        changed = true;
      }
    }
  }

  const resultMap = new Map<Node, Node | null>();
  postOrderNodes.forEach(data => {
    if (data.node != null) {
      resultMap.set(data.node, data.dominator!.node);
    }
  });
  return resultMap;
}
