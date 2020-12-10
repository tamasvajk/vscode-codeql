import * as React from 'react';
import { useState, useEffect, useRef } from 'react';
import * as Rdom from 'react-dom';

import {
  ToFlameGraphViewMessage,
} from '../../pure/interface-types';
import { vscode } from '../../view/vscode-api';
import { FlameGraphDraw } from './FlameGraphDraw';

const emptyMessage: ToFlameGraphViewMessage = {
  t: 'loadFlameGraph',
  data: {
    name: 'root',
    value: 0,
    children: []
  }
};

export function FlameGraph(_: {}): JSX.Element {
  const [logData, setLogData] = useState<ToFlameGraphViewMessage>(
    emptyMessage
  );

  // const message = logData.data.name || 'Empty data';
  const d3container = useRef(null);

  useEffect(() => {
    window.addEventListener('message', (evt: MessageEvent) => {
      if (evt.origin === window.origin) {
        const msg: ToFlameGraphViewMessage = evt.data;
        switch (msg.t) {
          case 'loadFlameGraph':
            console.log('Received message to load flame graph', msg);
            setLogData(msg);
        }
      } else {
        console.error(`Invalid event origin for flame graph view ${evt.origin}`);
      }
    });
    if (!(logData && d3container.current)) {
      return;
    }
  });

  if (!logData) {
    return <div>Waiting for graph to load.</div>;
  }
  return (
    <FlameGraphDraw {...logData.data} />
  );
}

Rdom.render(
  <FlameGraph />,
  document.getElementById('root'),
  // Post a message to the extension when fully loaded.
  () => vscode.postMessage({ t: 'flameGraphViewLoaded' })
);
