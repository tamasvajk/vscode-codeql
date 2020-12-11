import {
  window,
  TreeDataProvider,
  EventEmitter,
  Event,
  ProviderResult,
  TreeItemCollapsibleState,
  TreeItem,
  TreeView,
  Location,
  Uri,
  Position,
  Range,
} from 'vscode';
import * as path from 'path';

import { showLocation } from './interface-utils';
import { commandRunner } from './helpers';
import { DisposableObject } from './vscode-utils/disposable-object';
import { LogFile, PipelineEvaluation, PipelineStep, Query, RaPredicate, SourceLine, Stage } from './experimental/dataModel';
import { logger } from './logging';

export interface StructuredLogItem {
  label?: string;
  description?: string;
  location?: Location;
  children: ChildStructuredLogItem[];
}

export interface ChildStructuredLogItem extends StructuredLogItem {
  parent: ChildStructuredLogItem | StructuredLogItem;
}

/** Parser visitor to convert a `LogFile` from the data model into a tree with parent and child references. */
function convertLogFile(logFile: LogFile, logFilePath: string): StructuredLogItem {

  /** Helper function to convert locations. */
  function getLocation(startLine?: SourceLine, endLine?: SourceLine): Location | undefined {
    if (startLine && endLine) {
      // VS Code positions are 0-based, but the locations from the parser are 1-based.
      const range = new Range(new Position(startLine.lineNumber - 1, 0), new Position(endLine.lineNumber - 1, 0));
      return new Location(Uri.file(logFilePath), range);
    } else {
      return undefined;
    }
  }
  const item: StructuredLogItem = {
    label: `Structured log for ${path.basename(logFilePath)}`,
    children: []
  };
  /** Helper function to add a child item, with a reference to its parent. */
  function addChild(parent: StructuredLogItem, child: StructuredLogItem) {
    parent.children.push({ ...child, parent: parent });
  }
  logFile.queries.forEach(query => addChild(item, convertQuery(query)));
  return item;

  function convertQuery(query: Query): StructuredLogItem {
    const item: StructuredLogItem = {
      label: 'Query ' + query.name,
      location: getLocation(query.startLine, query.endLine),
      children: []
    };
    // Predicates without a known stage go into the final stage
    const stagedPredicates = new Set<RaPredicate>();
    query.stages.forEach(stage => stage.predicates.forEach(p => stagedPredicates.add(p)));
    query.raPredicates?.forEach(p => {
      if (!stagedPredicates.has(p)) {
        logger.log('Adding unstaged predicate ' + p.name + ' to final stage');
        query.stages[query.stages.length - 1].predicates.push(p);
      }
    });
    query.stages.forEach(stage => addChild(item, convertStage(stage)));
    return item;
  }
  function convertStage(stage: Stage): StructuredLogItem {
    const lines = (stage.startLine && stage.endLine) ? `(lines ${stage.startLine.lineNumber}-${stage.endLine.lineNumber})` : '';
    const item: StructuredLogItem = {
      label: `Stage ${stage.stageNumber} - ${stage.numTuples} tuples in ${stage.stageTime}s ${lines}`,
      location: getLocation(stage.startLine, stage.endLine),
      children: []
    };
    stage.predicates.forEach(predicate => addChild(item, convertPredicate(predicate)));
    return item;
  }
  function getTupleCountLabelSuffix(tupleCount?: number) {
    return tupleCount === undefined ? '' : ` - ${tupleCount} tuples`;
  }
  function convertPredicate(predicate: RaPredicate): StructuredLogItem {
    const item: StructuredLogItem = {
      label: predicate.name + getTupleCountLabelSuffix(predicate.rowCount),
      children: []
    };

    if (predicate.evaluationTime) {
      item.label! += ` in ${predicate.evaluationTime}ms`;
    }

    if (predicate.evaluations.length === 0) {
      item.label! += ' (from cache)';
    } else if (predicate.evaluations.length === 1) {
      // If there is only a single pipeline evaluation, point directly to its steps as the children of this node.
      // This avoids an unnecessary intermediate node.
      const evaluation = predicate.evaluations[0];
      evaluation.steps.forEach(step => addChild(item, convertPipelineStep(step)));
    } else {
      predicate.evaluations.forEach((evaluation, index) => addChild(item, convertPipelineEvaluation(`#${index}`, evaluation)));
    }
    return item;
  }
  function convertPipelineEvaluation(label: string, evaluation: PipelineEvaluation): StructuredLogItem {
    const item: StructuredLogItem = {
      label: label,
      location: getLocation(evaluation.lines[0], evaluation.lines[-1]),
      children: []
    };
    evaluation.steps.forEach(step => addChild(item, convertPipelineStep(step)));
    return item;
  }
  function convertPipelineStep(step: PipelineStep): StructuredLogItem {
    const item: StructuredLogItem = {
      label: `(${step.tupleCount} tuples) r${step.target}`,
      location: getLocation(step.line, step.line),
      description: `= ${step.body}`,
      children: []
    };
    // step.subPredicates.forEach(predicate => addChild(item, convertPredicate(predicate)));
    return item;
  }
}

/** Provides data from parsed CodeQL evaluation logs to be rendered in a tree view. */
class StructuredLogViewerDataProvider extends DisposableObject implements TreeDataProvider<StructuredLogItem> {

  public currentLog?: StructuredLogItem;

  private _onDidChangeTreeData =
    this.push(new EventEmitter<StructuredLogItem | undefined>());
  readonly onDidChangeTreeData: Event<StructuredLogItem | undefined> =
    this._onDidChangeTreeData.event;

  constructor() {
    super();
    this.push(
      commandRunner('codeQLStructuredLogViewer.gotoLog',
        async (item: StructuredLogItem) => {
          await showLocation(item.location);
        })
    );
  }

  refresh(): void {
    this._onDidChangeTreeData.fire();
  }
  getChildren(item?: StructuredLogItem): ProviderResult<StructuredLogItem[]> {
    // This method is called with no `item` to load the top-level tree.
    if (!item) {
      return this.currentLog ? [this.currentLog] : [];
    }
    // Otherwise it is called with an existing item, to load its children.
    return item.children;
  }

  getParent(item: ChildStructuredLogItem): ProviderResult<StructuredLogItem> {
    return item.parent;
  }

  getTreeItem(item: StructuredLogItem): TreeItem {
    const state = item.children.length
      ? TreeItemCollapsibleState.Collapsed
      : TreeItemCollapsibleState.None;
    const treeItem = new TreeItem(item.label || '', state);
    // TODO add more detail to tree items
    treeItem.description = item.description;
    // treeItem.id = String(item.id);
    treeItem.tooltip = `${treeItem.label} ${treeItem.description || ''}`;
    treeItem.command = {
      command: 'codeQLStructuredLogViewer.gotoLog',
      title: 'Go To Log',
      tooltip: `Go To ${item.location}`,
      arguments: [item]
    };
    return treeItem;
  }
}

/** Manages a tree view to render parsed CodeQL evaluation logs. */
export class StructuredLogViewer extends DisposableObject {
  private treeView: TreeView<StructuredLogItem>;
  private treeDataProvider: StructuredLogViewerDataProvider;

  constructor() {
    super();

    this.treeDataProvider = new StructuredLogViewerDataProvider();
    this.treeView = window.createTreeView('codeQLStructuredLogViewer', {
      treeDataProvider: this.treeDataProvider,
      showCollapseAll: true
    });

    this.push(this.treeView);
    this.push(this.treeDataProvider);
    this.push(
      commandRunner('codeQLStructuredLogViewer.clear', async () => {
        this.clear();
      })
    );
  }

  setCurrentLog(logFile: LogFile, logFilePath: string) {
    const log = convertLogFile(logFile, logFilePath);
    this.treeDataProvider.currentLog = log;
    this.treeDataProvider.refresh();
    this.treeView.message = `Structured log view for ${path.basename(logFilePath)}`;
    this.treeView.reveal(log, { focus: false });
  }

  private clear() {
    this.treeDataProvider.currentLog = undefined;
    this.treeDataProvider.refresh();
    this.treeView.message = undefined;
  }
}
