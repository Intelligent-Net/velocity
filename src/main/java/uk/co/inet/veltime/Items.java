package uk.co.inet.veltime;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import static java.util.stream.Collectors.joining;

class Items extends Base implements Comparable, Serializable
{
  public Item[] items;

  public Items(Item[] items)
  {
    this.items = items;
  }

  public String toString()
  {
    return Arrays.asList(items).stream().map(Item::toString).collect(joining(","));
  }

  public int compareTo(Object o)
  {
    int same = 0;

    if (o instanceof Items)
    {
      Items other = ((Items) o); 

      if (other.items.length != items.length)
        same = -1;
      else
      {
        for (int i = 0; i != items.length; i++)
          if (other.items[i].compareTo(items[i]) != 0)
          {
            same = 1;

            break;
          }
      }
    }
    else
      same = -1;

    return same;
  }
}
