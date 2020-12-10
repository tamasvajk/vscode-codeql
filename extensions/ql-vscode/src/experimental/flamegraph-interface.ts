import { DisposableObject } from '../vscode-utils/disposable-object';
import {
  WebviewPanel,
  ExtensionContext,
  window as Window,
  ViewColumn,
  Uri,
} from 'vscode';
import * as path from 'path';

import { tmpDir } from '../run-queries';
import {
  FromFlameGraphViewMessage,
  ToFlameGraphViewMessage
} from '../pure/interface-types';
import { Logger } from '../logging';
import { getHtmlForWebview } from '../interface-utils';
import { CompletedQuery } from '../query-results';
import { readFile } from './flamegraph_builder';

export class FlameGraphInterfaceManager extends DisposableObject {
  private panel: WebviewPanel | undefined;
  private panelLoaded = false;
  private panelLoadedCallBacks: (() => void)[] = [];

  constructor(
    private ctx: ExtensionContext,
    private logger: Logger
  ) {
    super();
  }

  async showFlameGraph(query: CompletedQuery) {
    if (!query.logFileLocation) {
      this.logger.log('Query has no log file');
      return;
    }
    this.getPanel().reveal(undefined, true);
    await this.waitForPanelLoaded();
    this.logger.log('Building flame graph from ' + query.logFileLocation);
    const flameGraph = await readFile(query.logFileLocation);
    this.logger.log('Sending log data to flame graph viewer');
    await this.postMessage({
      t: 'loadFlameGraph',
      data: flameGraph
    });
  }

  getPanel(): WebviewPanel {
    if (this.panel == undefined) {
      const { ctx } = this;
      const panel = (this.panel = Window.createWebviewPanel(
        'flameGraphView',
        'View CodeQL performance flame graph',
        { viewColumn: ViewColumn.Active, preserveFocus: true },
        {
          enableScripts: true,
          enableFindWidget: true,
          retainContextWhenHidden: true,
          localResourceRoots: [
            Uri.file(tmpDir.name),
            Uri.file(path.join(this.ctx.extensionPath, 'out')),
          ],
        }
      ));
      this.panel.onDidDispose(
        () => {
          this.panel = undefined;
        },
        null,
        ctx.subscriptions
      );

      const scriptPathOnDisk = Uri.file(
        ctx.asAbsolutePath('out/flameGraphView.js')
      );

      const stylesheetPathOnDisk = Uri.file(
        ctx.asAbsolutePath('out/resultsView.css')
      );

      panel.webview.html = getHtmlForWebview(
        panel.webview,
        scriptPathOnDisk,
        stylesheetPathOnDisk
      );
      panel.webview.onDidReceiveMessage(
        async (e) => this.handleMsgFromView(e),
        undefined,
        ctx.subscriptions
      );
    }
    return this.panel;
  }

  private waitForPanelLoaded(): Promise<void> {
    return new Promise((resolve) => {
      if (this.panelLoaded) {
        resolve();
      } else {
        this.panelLoadedCallBacks.push(resolve);
      }
    });
  }

  private async handleMsgFromView(
    msg: FromFlameGraphViewMessage
  ): Promise<void> {
    switch (msg.t) {
      case 'flameGraphViewLoaded':
        this.panelLoaded = true;
        this.panelLoadedCallBacks.forEach((cb) => cb());
        this.panelLoadedCallBacks = [];
        break;
    }
  }

  private postMessage(msg: ToFlameGraphViewMessage): Thenable<boolean> {
    return this.getPanel().webview.postMessage(msg);
  }
}
