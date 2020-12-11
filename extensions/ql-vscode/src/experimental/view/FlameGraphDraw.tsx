import * as React from 'react';
import { useEffect, useRef } from 'react';
import { select } from 'd3-selection';
import * as d3f from 'd3-flame-graph';

import { FlameGraphNode } from '../flamegraph_builder';

export function FlameGraphDraw(props: FlameGraphNode): JSX.Element {

  const d3container = useRef(null);

  useEffect(() => {
    if (!(props && d3container.current)) {
      return;
    }

    const svg = select(d3container.current);

    const chart = d3f.flamegraph();
    // .width(1000);
    // .onClick((event: D3Node) => {
    // focusedNode = event.data;
    // showDetailsForNode(focusedNode);
    // });

    svg.datum(props).call(chart);

  }, [props, d3container.current]);

  return (
    <>
      <div className="vscode-codeql__flame-graph-header" style={{ textAlign: 'center', overflowY: 'scroll' }}>
        <div style={{ width: 1000, height: 1000 }}>
          <svg className="d3-component" width={'90%'} height={'auto'} ref={d3container} />
        </div>
      </div>
    </>
  );
}
