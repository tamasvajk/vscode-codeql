export type Collection<T> = T[] | Set<T>;

export function getArray<K, V>(map: Map<K, V[]>, key: K): V[] {
  let result = map.get(key);
  if (result == null) {
    result = [];
    map.set(key, result);
  }
  return result;
}

export function getMap<K1, K2, V>(map: Map<K1, Map<K2, V>>, key: K1): Map<K2, V> {
  let result = map.get(key);
  if (result == null) {
    result = new Map();
    map.set(key, result);
  }
  return result;
}

export function getSet<K, V>(map: Map<K, Set<V>>, key: K): Set<V> {
  let result = map.get(key);
  if (result == null) {
    result = new Set();
    map.set(key, result);
  }
  return result;
}

export function multiMapRemove<K, V>(map: Map<K, Set<V>>, key: K, value: V): boolean {
  const set = map.get(key);
  if (set == null) { return false; }
  if (set.delete(value)) {
    if (set.size === 0) {
      map.delete(key);
    }
    return true;
  }
  return false;
}

export function mapIncrement<K>(map: Map<K, number>, key: K, delta: number) {
  const value = map.get(key) ?? 0;
  map.set(key, value + delta);
}

export function getInverse<K, V>(map: Map<K, V>): Map<V, K[]> {
  const result = new Map<V, K[]>();
  map.forEach((value, key) => {
    getArray(result, value).push(key);
  });
  return result;
}

export function asArray<T>(x: T | T[] | null | undefined): T[] {
  if (x == null) { return []; }
  if (Array.isArray(x)) { return x; }
  return [x];
}

export function withoutNulls<T>(array: readonly T[]): NonNullable<T>[] {
  return array.filter(x => x != null) as NonNullable<T>[];
}

export const doNothing = () => undefined;

/**
 * Returns the index of the last element of `array` that is less than or equal to `value`,
 * or -1 if all elements are greater than `value`.
 *
 * The array must be sorted and non-empty.
 */
export function getLowerBound(array: readonly number[], value: number) {
  let low = 0, high = array.length - 1;
  if (value < array[0]) { return -1; }
  if (value >= array[high]) { return high; }
  while (low < high) {
    const mid = high - ((high - low) >> 1); // Get middle, rounding up.
    if (value < array[mid]) {
      high = mid - 1;
    } else {
      low = mid;
    }
  }
  return low;
}
