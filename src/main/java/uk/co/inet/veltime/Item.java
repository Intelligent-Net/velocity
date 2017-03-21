package uk.co.inet.veltime;

import java.io.*;

class Item extends Base implements Comparable, Serializable
{
  public int ts;
  // item is either a fixed point (2 places) amount for velocity with amount
  // or a count for changes
  public int item;

  public Item(int ts)
  {
    this.ts = ts;
    this.item = 1;
  }

  public Item(int ts, int item)
  {
    this.ts = ts;
    this.item = item;
  }

  public Item(int ts, double item)
  {
    this.ts = ts;
    this.item = Vel.toFix(item);
  }

  public String toString()
  {
    return ts + "|" + item;
  }

  public int compareTo(Object o)
  {
    if (o instanceof Item)
    {
      Item it = (Item) o;

      if (ts == it.ts)
        return item - it.item;
      else
        return ts - it.ts;
    }
    else
      return -1;
  }
}
