import { FromCompareViewMessage, FromFlameGraphViewMessage, FromResultsViewMsg } from '../pure/interface-types';

export interface VsCodeApi {
  /**
   * Post message back to vscode extension.
   */
  postMessage(msg: FromResultsViewMsg | FromCompareViewMessage | FromFlameGraphViewMessage): void;
}

declare const acquireVsCodeApi: () => VsCodeApi;
export const vscode = acquireVsCodeApi();
