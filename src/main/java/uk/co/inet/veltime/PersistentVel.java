package uk.co.inet.veltime;

import java.io.*;

import java.net.*;
import java.text.*;
import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;

import org.nustaq.serialization.*;
import org.nustaq.serialization.util.FSTInt2IntMap;
import org.nustaq.offheap.*;
import org.nustaq.offheap.bytez.bytesource.BytezByteSource;
import org.nustaq.offheap.bytez.*;

// return class for standard velocity so all calculations can be done once
public class PersistentVel extends Vel implements Serializable
{
/**
 *  A class to handle general Persistence of short term data locally on the
 *  machine. It's very fast and suitable for tasks such as velocity over a
 *  short time frame (typically 24 hours) for which support functions are
 *  supplied. Other velocity tasks may require longer time intervals for which
 *  a traditional database would be more suitable.
 *
 *  This class also contains methods to support velocity. velocity can be done
 *  on any arbitrary key and value. The key is the particular objects (or
 *  set of objects) on which a velocity test is being done. If the value matches
 *  then the velocity has succeeded and a counter is incremented.
 *
 *  There are two types of counters available which can be used individually or
 *  together, namely a simple count of matches and a monetary value match,
 *  where the monetary value can change on each call. There is also methods
 *  where if the key matches then the value should match too, if it does
 *  not then the counter is increased.
 */

  private FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
  private FSTLongOffheapMap<byte[]> tm;

  public PersistentVel(String nm)
  {
    type = "PersistentVel";

    saveParas = true;

    try
    {
      new File("veldb").mkdir();
    }
    catch (Exception e)
    {
      // Ignore, may already exist
    }

    try
    {
      new File("veldb").mkdir();
      //tm = new FSTLongOffheapMap<>(saveDir + nm, 2 * FSTLongOffheapMap.GB, limit);
      tm = new FSTLongOffheapMap<>(saveDir + nm, FSTLongOffheapMap.GB / 16, 0);
    }
    catch (Exception e)
    {
      System.err.println(e);
    }
  }

  protected Base get(long key)
  {
    byte[] bs = tm.get(key);

    if (bs == null)
      return null;

    return Utils.toBase(bs);
  }

  protected Base removeKey(long key)
  {
    byte[] bs = tm.get(key);

    if (bs == null)
      return null;

    tm.remove(key);

    return Utils.toBase(bs);
  }

  public static void init(String mn)
  {
    init(mn, null, false);
  }

  public static void init(String mn, String fn, boolean ro)
  {
    if (! maps.containsKey(mn))
    {
      PersistentVel p = new PersistentVel(mn);

      maps.put(mn, p);
    }
  }

  public static Vel context(String db)
  {
    Vel ctx = context(db, "PersistentVel");

    ctx.nm = db;

    return ctx;
  }

  public String show(String key)
  {
    return tm.get(vhash(key)).toString();
  }

  public synchronized Base set(long key, Base item)
  {
    if (item == null)
      tm.remove(key);
    else
    {
      byte[] bs = Utils.fromBase(item);

      tm.put(key, bs);
    }

    return item;
  }

  /**
   *  show the database
   *
   * @return    The string representation of the className
   */
  public String toString()
  {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;

    for (Iterator i = tm.binaryKeys(); i.hasNext();)
    {
      BytezByteSource bbs = (BytezByteSource) i.next();
      Bytez bz = bbs.getBytes();
      long key = bz.getLong(bbs.getOff());

      if (! first)
        sb.append(",");
      else
        first = false;

      Base base = Utils.toBase(tm.get(key));

      sb.append(showItem(key, base));
    }

    sb.append("}");

    return sb.toString();
  }

  // Copy from one implementation to another
  public Vel copy(String type, String nm, boolean merge)
  {
    Vel p = getVel(type, nm);

    if (p != null)
    {
      for (Iterator i = tm.binaryKeys(); i.hasNext();)
      {
        BytezByteSource bbs = (BytezByteSource) i.next();
        Bytez bz = bbs.getBytes();
        long key = bz.getLong(bbs.getOff());
        Base base = Utils.toBase(tm.get(key));

        if (merge)
          p.merge(key, base, max);
        else
          p.set(key, base);
      }
    }

    return p;
  }

  // count records and display every Nth record
  public int count(int n)
  {
    int cnt = 0;

    for (Iterator i = tm.binaryKeys(); i.hasNext();)
    {
      BytezByteSource bbs = (BytezByteSource) i.next();

      if ((cnt % n) == 0)
      {
        Bytez bz = bbs.getBytes();
        long key = bz.getLong(bbs.getOff());
        Base base = Utils.toBase(tm.get(key));
        String item = showItem(key, base);

        System.out.println(cnt + " : " + item);
      }

      cnt++;
    }

    return cnt;
  }

  protected synchronized int purgeDb(int now)
  {
    int its = 0;
    int cnt = 0;
    int n = 0;
    long[] keys = new long[100000];
    boolean first = true;

    while (its == 0 || n > 0)
    {
      if (n == keys.length)
      {
//System.err.println("--- " + n);
        for (int i = 0; i != n; i++)
          tm.remove(keys[i]);

        n = 0;
      }

      int tot = 0;
      int sofar = its * keys.length;

      for (Iterator i = tm.binaryKeys(); n < keys.length && i.hasNext();)
      {
        BytezByteSource bbs = (BytezByteSource) i.next();

        // Skip forward
        if (sofar > tot++)
          continue;

        Bytez bz = bbs.getBytes();
        long key = bz.getLong(bbs.getOff());
        Base val = Utils.toBase(tm.get(key));
        int dt = 0;

        if (val instanceof Item)
          dt = ((Item) val).ts;
        else if (((Items) val).items[0] != null)
          dt = ((Items) val).items[0].ts;

        if (dt > 20000000 && now > dt)
        {
          //i.remove(); // NOT IMPLEMENTED

          keys[n++] = key;
          cnt++;
        }
      }

      its++;
    }

    for (int i = 0; i != n; i++)
      tm.remove(keys[i]);

    return cnt;
  }

  public int recCount()
  {
    return tm.getSize();
  }

  public String stats()
  {
    return tm.getCapacityMB() + "|" + (tm.getFreeMem() / 1024 / 1024) + "|" + (tm.getUsedMem() / 1024 / 1024) ;
  }
}
