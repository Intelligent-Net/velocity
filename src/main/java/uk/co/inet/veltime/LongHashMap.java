package uk.co.inet.veltime;

import java.io.Serializable;
import java.util.Map;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Minimal Hash Map which uses primitive long as key.
 * Main advantage is new instanceof of Long does not have to be created for each lookup.
 * <p/>
 * This code comes from Android, which in turns comes from Apache Harmony.
 * This class was modified to use primitive longs and stripped down to consume less space.
 */
public class LongHashMap<V> {

  private int elementCount;

  private Entry<V>[] elementData;

  private final float loadFactor;

  private int threshold;

  private int defaultSize = 1024;

  static final class Entry<V>
  {
    long key;
    V value;
    Entry<V> next;

    Entry(long theKey) {
      this.key = theKey;
      this.value = null;
    }
  }

  static class HashMapIterator<V> implements Iterator<V> {
    private int position = 0;

    boolean canRemove = false;

    Entry<V> entry;

    Entry<V> lastEntry;

    final LongHashMap<V> associatedMap;

    public HashMapIterator(LongHashMap<V> hm) {
      associatedMap = hm;
    }

    public boolean hasNext() {
      if (entry != null) {
        return true;
      }

      Entry<V>[] elementData = associatedMap.elementData;
      int length = elementData.length;
      int newPosition = position;
      boolean result = false;

      while (newPosition < length) {
        if (elementData[newPosition] == null) {
          newPosition++;
        } else {
          result = true;
          break;
        }
      }

      position = newPosition;
      return result;
    }

    public V next() {

      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      Entry<V> result;
      Entry<V> _entry = entry;
      if (_entry == null) {
        result = lastEntry = associatedMap.elementData[position++];
        entry = lastEntry.next;
      } else if (lastEntry != null) {
        if (lastEntry.next != _entry) {
          lastEntry = lastEntry.next;
        }
        result = _entry;
        entry = _entry.next;
      }
      else
        return null;
      canRemove = true;
      return result.value;
    }

    public Entry<V> nextEntry() {

      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      Entry<V> result;
      Entry<V> _entry = entry;
      if (_entry == null) {
        result = lastEntry = associatedMap.elementData[position++];
        entry = lastEntry.next;
      } else {
        if (lastEntry.next != _entry) {
          lastEntry = lastEntry.next;
        }
        result = _entry;
        entry = _entry.next;
      }
      canRemove = true;
      return result;
    }

    public void remove() {
      if (!canRemove) {
        throw new IllegalStateException();
      }

      canRemove = false;

      if (lastEntry != null) {
        if (lastEntry.next == entry) {
          while (associatedMap.elementData[--position] == null) {
            // Do nothing
          }
          associatedMap.elementData[position] = associatedMap.elementData[position].next;
          entry = null;
        } else {
          lastEntry.next = entry;
        }
      }
      lastEntry = null;

      associatedMap.elementCount--;
    }
  }


  /**
   * Constructs a new empty {@code HashMap} instance.
   *
   * @since Android 1.0
   */
  public LongHashMap() {
    this(1024);
  }

  /**
   * Constructs a new {@code HashMap} instance with the specified capacity.
   *
   * @param capacity the initial capacity of this hash map.
   * @throws IllegalArgumentException when the capacity is less than zero.
   * @since Android 1.0
   */
  @SuppressWarnings("unchecked")
  public LongHashMap(int capacity) {
    defaultSize = capacity;
    if (capacity >= 0) {
      elementCount = 0;
      //elementData = newElementArray(capacity == 0 ? 1 : capacity);
      elementData = new Entry[capacity == 0 ? 1 : capacity];
      loadFactor = 0.7f; // Default load factor of 0.7
      computeMaxSize();
    } else {
      throw new IllegalArgumentException();
    }
  }


  /**
   * Removes all mappings from this hash map, leaving it empty.
   *
   * @see #isEmpty
   * @see #size
   * @since Android 1.0
   */

  @SuppressWarnings("unchecked")
  public void clear() {
    if (elementCount > 0) {      
      elementCount = 0;      
    }
    if(elementData.length>1024 && elementData.length>defaultSize)
      elementData = new Entry[defaultSize];
    else
      Arrays.fill(elementData, null);
    computeMaxSize();
  }

  /**
   * Returns a shallow copy of this map.
   *
   * @return a shallow copy of this map.
   * @since Android 1.0
   */


  private void computeMaxSize() {
    threshold = (int) (elementData.length * loadFactor);
  }


  /**
   * Returns the value of the mapping with the specified key.
   *
   * @param key the key.
   * @return the value of the mapping with the specified key, or {@code null}
   *     if no mapping for the specified key is found.
   * @since Android 1.0
   */

  public V get(final long key) {

    final int hash = powerHash(key);
    final int index = (hash & 0x7FFFFFFF) % elementData.length;

    //find non null entry
    Entry<V> m = elementData[index];
    while (m != null) {
      if (key == m.key)
        return m.value;
      m = m.next;
    }

    return null;

  }

  public V get(final String key) {
    return get(Vel.vhash(key));
  }

  /**
   * Returns whether this map is empty.
   *
   * @return {@code true} if this map has no elements, {@code false}
   *     otherwise.
   * @see #size()
   * @since Android 1.0
   */

  public boolean isEmpty() {
    return elementCount == 0;
  }

  /**
   * Maps the specified key to the specified value.
   *
   * @param key   the key.
   * @param value the value.
   * @return the value of any previous mapping with the specified key or
   *     {@code null} if there was no such mapping.
   * @since Android 1.0
   */


  public V put(final long key, final V value) {

    int hash = powerHash(key);
    int index = (hash & 0x7FFFFFFF) % elementData.length;

    //find non null entry
    Entry<V> entry = elementData[index];
    while (entry != null && key != entry.key) {
      entry = entry.next;
    }

    if (entry == null) {
      if (++elementCount > threshold) {
        rehash();
        index = (hash & 0x7FFFFFFF) % elementData.length;
      }
      entry = createHashedEntry(key, index);
    }


    V result = entry.value;
    entry.value = value;
    return result;
  }

  public V put(final String key, final V value) {
    return put(Vel.vhash(key), value);
  }

  Entry<V> createHashedEntry(final long key, final int index) {
    Entry<V> entry = new Entry<V>(key);

    entry.next = elementData[index];

    elementData[index] = entry;

    return entry;
  }


  @SuppressWarnings("unchecked")
  void rehash(final int capacity) {
    int length = (capacity == 0 ? 1 : capacity << 1);

    //Entry<V>[] newData = newElementArray(length);
    Entry<V>[] newData = new Entry[length];
    for (int i = 0; i < elementData.length; i++) {
      Entry<V> entry = elementData[i];
      while (entry != null) {
        int index = (powerHash(entry.key) & 0x7FFFFFFF) % length;
        Entry<V> next = entry.next;
        entry.next = newData[index];
        newData[index] = entry;
        entry = next;
      }
    }
    elementData = newData;
    computeMaxSize();
  }

  void rehash()
  {
    rehash(elementData.length);
  }

  /**
   * Removes the mapping with the specified key from this map.
   *
   * @param key the key of the mapping to remove.
   * @return the value of the removed mapping or {@code null} if no mapping
   *     for the specified key was found.
   * @since Android 1.0
   */

  public V remove(final long key)
  {
    Entry<V> entry = removeEntry(key);

    if (entry == null)
      return null;

    return entry.value;
  }

  public V remove(final String key) {
    return remove(Vel.vhash(key));
  }

  Entry<V> removeEntry(final long key) {
    Entry<V> last = null;

    final int hash = powerHash(key);
    final int index = (hash & 0x7FFFFFFF) % elementData.length;
    Entry<V> entry = elementData[index];

    while (true) {
      if (entry == null) {
        return null;
      }

      if (key == entry.key) {
        if (last == null) {
          elementData[index] = entry.next;
        } else {
          last.next = entry.next;
        }
        elementCount--;
        return entry;
      }

      last = entry;
      entry = entry.next;
    }
  }

  Entry<V> removeEntry(final String key) {
    return removeEntry(Vel.vhash(key));
  }

  /**
   * Returns the number of elements in this map.
   *
   * @return the number of elements in this map.
   * @since Android 1.0
   */

  public int size() {
    return elementCount;
  }

  /**
   * @returns iterator over values in map
   */
  public Iterator<V> valuesIterator() {
    return new HashMapIterator<V>(this);
  }

  static final private int powerHash(final long key){
    int h = (int)(key ^ (key >>> 32));
    h ^= (h >>> 20) ^ (h >>> 12);
    return h ^ (h >>> 7) ^ (h >>> 4);
  }
}
