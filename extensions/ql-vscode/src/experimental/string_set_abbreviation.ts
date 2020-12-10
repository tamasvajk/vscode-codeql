import { Collection } from './util';

/**
 * Returns a string whose brace expansion would result in the given set of strings.
 *
 * Braces are only inserted following a `::` token.
 *
 * For example, the strings `foo::bar` and `foo:baz` would abbreviate to `foo::{bar, baz}`, and not `foo::ba{r,z}`.
 */
export function abbreviateStrings(strings: Collection<string>): string {
  type TrieNode = Map<string, TrieNode>;

  const trie = new Map<string, TrieNode>();

  strings.forEach((str: string) => {
    const parts = str.split('::');
    let currentNode = trie;
    for (const part of parts) {
      const value = currentNode.get(part);
      if (value == null) {
        const map = new Map<string, TrieNode>();
        currentNode.set(part, map);
        currentNode = map;
      } else {
        currentNode = value;
      }
    }
  });

  function stringifyNode(node: TrieNode): string {
    const parts: string[] = [];
    node.forEach((value, key) => {
      parts.push(key + stringifyNodeSuffix(value));
    });
    return parts.join(', ');
  }

  function stringifyNodeSuffix(node: TrieNode): string {
    if (node.size === 0) {
      return '';
    } else if (node.size > 1) {
      return '::{' + stringifyNode(node) + '}';
    } else {
      return '::' + stringifyNode(node);
    }
  }

  return stringifyNode(trie);
}
