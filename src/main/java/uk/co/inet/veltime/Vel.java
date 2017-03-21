package uk.co.inet.veltime;

import java.io.*;
import java.net.*;
import java.text.*;
import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.net.ConnectException;
import java.lang.reflect.Constructor;

import static java.lang.Integer.rotateLeft;
import static java.util.stream.Collectors.joining;

public abstract class Vel
{
  // Defaults etc
  protected static final int VEL_HITS = 3;
  protected static final double VAL_HITS = 0.0;
  protected static final int VEL_DURATION = 60;  // Minutes
  protected static final int MAX = 10;
  protected static final String DIR = "veldb";
  protected static Logger logger;

  // Variables to be saved for persistent version
  protected int hitMax = VEL_HITS;
  protected double valMax = VAL_HITS;
  protected int velDur = VEL_DURATION;
  protected int max = MAX;
  protected String dir = DIR;
  protected String saveDir = dir + File.separator;

  // Timers and counters
  protected final long startTime = System.currentTimeMillis();
  private final AtomicLong lastPurge = new AtomicLong();
  private final AtomicLong purged = new AtomicLong();
  private final AtomicInteger purgerCalls = new AtomicInteger();
  private final AtomicInteger purger = new AtomicInteger();

  protected final static Map<String,Vel> maps = new ConcurrentHashMap<>(1000);

  protected String type;
  protected String nm = "map";

  // Property saving stuff
  private String mn;
  private String fn;

  protected boolean saveParas = false;

  /**
   *  Set the logger for error messages
   *
   * @param  logger    The logger to use for errors
   */
  public static void setLogger(Logger logger)
  {
    Vel.logger = logger;
  }

  /**
   *  Obscure a data item using a one way algorithm for later comparision
   *
   * @param  s        The data item to obscure as a string
   * @return        The obscured string
   */
  public final static int obscure(String s)
  {
    byte[] b = s.getBytes();

    int i = hash(b, b.length * b[0] * b[b.length - 1]);

    if (i < 0)
      i = -i;

    return i;
  }

  public static final long vhash(String s)
  {
    if (s == null || s.length() == 0)
      return 0L;

    byte[] b = s.getBytes();

    //int i1 = hash(b, b.length * b[0]);
    int i1 = hash(b, b.length);

    if (i1 < 0)
      i1 = -i1;

    //int i2 = hash(b, i1);
    int i2 = s.hashCode();

    long ind = (((long) i1) << 32) | (i2 & 0xFFFFFFFFL);

    if (ind < 0)      // should never happen
      ind = - ind;

    return ind;
  }

  protected void saveProp()
  {
    Properties prp = new Properties();

    prp.put("hitMax", Integer.toString(hitMax));
    prp.put("valMax", Double.toString(valMax));
    prp.put("velDur", Integer.toString(velDur));
    prp.put("max", Integer.toString(max));

    try (Writer w = new FileWriter(fn + ".prp"))
    {
      prp.store(w, mn);
    }
    catch (IOException ioe)
    {
      // Ignore
    }
  }

  protected void loadProp()
  {
    Properties prp = new Properties();

    try (Reader r = new FileReader(fn + ".prp"))
    {
      prp.load(r);
    }
    catch (IOException ioe)
    {
      // Ignore
    }

    hitMax = Integer.parseInt(prp.getProperty("hitMax", Integer.toString(hitMax)));
    valMax = Double.parseDouble(prp.getProperty("valMax", Double.toString(valMax)));
    velDur = Integer.parseInt(prp.getProperty("velDur", Integer.toString(velDur)));
    max = Integer.parseInt(prp.getProperty("max", Integer.toString(max)));
  }

  public synchronized void close()
  {
    // Do nothing - overridden if necessary
  }

  /**
   *  Set the instance hitMax
   *
   * @param  hitMax     The hit max is the velocity count threshold over
   *       which a velocity match is deemed to have occurred
   */
  public Vel setHitMax(int hitMax)
  {
    if (this.hitMax != hitMax)
    {
      this.hitMax = hitMax;

      if (saveParas)
        saveProp();
    }

    return this;
  }

  /**
   *  Set the instance valMax
   *
   * @param  valMax     The val max is the velocity value threshold over
   *       which a velocity match is deemed to have occurred
   */
  public Vel setValMax(double valMax)
  {
    if (this.valMax != valMax)
    {
      this.valMax = valMax;

      if (saveParas)
        saveProp();
    }

    return this;
  }

  /**
   *  Set the instance velDur (default 1 hour)
   *
   * @param  velDur     The velocity duration is the time interval
   *       (measured in minutes) over which the moving window
   *       of velocity operates. If a hit on the same velocity
   *       component(s) occurs within the window then the count
   *       or value will be incremented. If it is not within
   *       the window then the count will be reset to 0.
   */
  public Vel setVelDur(int velDur)
  {
    if (this.velDur != velDur)
    {
      this.velDur = velDur;

      if (saveParas)
        saveProp();
    }

    return this;
  }

  public boolean getSaveParas()
  {
    return saveParas;
  }

  public Vel setSaveParas(boolean saveParas)
  {
    this.saveParas = saveParas;

    return this;
  }

  public int getMax()
  {
    return max;
  }

  public Vel setMax(int max)
  {
    if (this.max != max)
    {
      this.max = max;

      if (saveParas)
        saveProp();
    }

    return this;
  }

  abstract public String show(String key);

  protected static String name()
  {
    return new Throwable().getStackTrace()[0].getClassName();
  }

  public static int getMapSize()
  {
    return maps.size();
  }

  public static String[] getMaps()
  {
    String[] map = new String[maps.size()];

    int i = 0;

    for (Map.Entry<String,Vel> e : maps.entrySet())
    {
      map[i] = e.getValue().nm;

      i++;
    }

    return map;
  }

  public static int duration(String db)
  {
    Vel p = context(db);

    return (int) ((System.currentTimeMillis() - p.startTime) / 1000L / 60L);
  }

  public static int contextCalls(String db)
  {
    Vel p = context(db);

    return p.purger.get() * p.purgerCalls.get();
  }

  public static long purgedRecs(String db)
  {
    return context(db).purged.get();
  }

  public static Vel context(String db)
  {
    return context(db, null);
  }

  protected static Vel context(String db, String imp)
  {
    Vel p = maps.get(db);

    if (p != null)
    {
      if (p.purger.getAndIncrement() > 100000)
      {
        long pTime = System.currentTimeMillis();

        if (pTime > p.lastPurge.get())
        {
          int done = p.purgeDb();

          p.purged.set(p.purged.get() + done);
          p.purgerCalls.incrementAndGet();
        }

        p.lastPurge.set(pTime + 60 * 1000);
        p.purger.set(0);
      }

      return p;
    }

    if (imp != null)
    {
      try
      {
        String fqn = Vel.class.getName();
        Class c = Class.forName(fqn.substring(0, fqn.lastIndexOf('.') + 1) + imp);
        Constructor<?> cs = c.getConstructor(String.class);

        p = (Vel) cs.newInstance(db);

        maps.put(db, p);

        p.nm = db;

        p.open(db, null, false);  // flexible audit opening required
      }
      catch (Exception exc)
      {
        log("Instance creation failed", exc);
      }
    }

    return p;
  }

  // Dummy open - override/extend for disk based maps
  protected String open(String mn, String fn, boolean ro)
  {
    if (fn == null)
      fn = mn;

    if (! fn.startsWith(File.separator))
      fn = saveDir + fn;

    int i = fn.lastIndexOf(File.separator);

    if (i > 0)
    {
      saveDir = fn.substring(0, i + 1);

      if (saveParas)
        new File(fn.substring(0, i)).mkdirs();
    }

    this.mn = mn;
    this.fn = fn;

    loadProp();

    return fn;
  }

  // Dummy commit - override for disk based maps
  protected void commit()
  {
  }

  // Dummy rollback - override for disk based maps
  protected void rollback()
  {
  }
  
  public static String show(String db, String key)
  {
    Vel vel = context(db);

    return vel != null ? vel.show(key) : null;
  }

  abstract public Base set(long key, Base item);

  public static Base set(String db, String key, Base item)
  {
    Vel vel = context(db);

    return vel != null ? vel.set(vhash(key), item) : null;
  }

  protected static void log(String mess, Exception ex)
  {
    mess += " : ";

    if (logger != null)
      logger.log(Level.SEVERE, mess, ex);
    else
      System.err.println(mess + ex);
  }

  protected static void log(String mess)
  {
    if (logger != null)
      logger.log(Level.SEVERE, mess);
    else
      System.err.println(mess);
  }

  private Base parseBits(String bits)
  {
    String[] items = bits.split(",");

    if (items.length == 1)
    {
      String[] is = items[0].split("|");

      return new Item(Integer.parseInt(is[0]), Integer.parseInt(is[1]));
    }
    else
    {
      Item[] is = new Item[items.length];

      for (int j = 0; j != items.length; j++)
      {
        String[] i = items[j].split("|");

        is[j++] = new Item(Integer.parseInt(i[0]), Integer.parseInt(i[1]));
      }

      return new Items(is);
    }
  }

  public int importRecs(String fn)
  {
    int i = 0;

    try (BufferedReader fbr = new BufferedReader(new FileReader(fn)))
    {
      String line;

      while ((line = fbr.readLine()) != null)
      {
        String [] bits = line.split(":");

        long key = Long.parseLong(bits[0]);
        Base item = parseBits(bits[1]);

        set(key, item);

        i++;
      }
    }
    catch (Exception e)
    {
      log("import problem", e);
    }

    return i;
  }

  public static int importRecs(String db, String fn)
  {
    Vel vel = context(db);

    return vel != null ? vel.importRecs(fn) : null;
  }

  /**
   *  show the database
   *
   * @return    The string representation of the className
   */
  abstract public String toString();

  public static String toString(String db)
  {
    Vel vel = context(db);

    return vel != null ? vel.toString() : null;
  }

  protected static String showItem(Long key, Base item)
  {
    StringBuilder sb = new StringBuilder();
    boolean first = true;

    Item[] vals;

    if (item instanceof Item)
    {
      vals = new Item[1];

      vals[0] = (Item) item;
    }
    else
      vals = ((Items) item).items;

    for (Item val : vals)
    {
      if (! first)
        sb.append(";");
      else
      {
        sb.append(key.toString());
        sb.append("=");
      }

      if (val.ts > 20000000)
      {
        Date d = new Date(val.ts * 60000L);

        sb.append(d.toString());
        sb.append("|");
      }
      else
      {
        sb.append(val.ts);
        sb.append(":");
      }

      sb.append(val.item);

      first = false;
    }

    return sb.toString();
  }

  protected List<Long> getTs(String key)
  {
    return getTs(key, true);
  }

  protected List<Long> getExpiryTs(String key)
  {
    return getTs(key, false);
  }

  private List<Long> getTs(String key, boolean original)
  {
    long lkey = vhash(key);
    Base item = get(lkey);
    List<Long> ll = new ArrayList<>();

    if (item instanceof Item)
    {
      long ts = (long) ((Item) item).ts;

      if (original)
        ts -= velDur * 60 * 1000;

      ll.add(ts * 60L * 1000L);
    }
    else
    {
      for (Item val : ((Items) item).items)
      {
        long ts = (long) val.ts;

        if (original)
          ts -= velDur * 60 * 1000;

        ll.add(ts * 60L * 1000L);
      }
    }

    return ll;
  }

  protected SortedMap<Long, Integer> getTsCounts(String key)
  {
    return getTsCounts(key, "M", true);
  }

  protected SortedMap<Long, Integer> getTsCounts(String key, String unit)
  {
    return getTsCounts(key, unit, true);
  }

  protected SortedMap<Long, Integer> getExpiryTsCounts(String key)
  {
    return getTsCounts(key, "M", false);
  }

  protected SortedMap<Long, Integer> getExpiryTsCounts(String key, String unit)
  {
    return getTsCounts(key, unit, false);
  }

  private SortedMap<Long, Integer> getTsCounts(String key, String unit, boolean original)
  {
    long lkey = vhash(key);
    Base item = get(lkey);
    SortedMap<Long, Integer> ll = new TreeMap<>();

    if (unit == null || unit.isEmpty())
      unit = "M";
    else
      unit = unit.toUpperCase();

    int div;

    switch (unit.charAt(0))
    {
      case 'H' : div = 60; break;
      case 'D' : div = 60 * 24; break;
      case 'W' : div = 60 * 24 * 7; break;
      default : div = 1; break;
    }

    div *= 60 * 1000;

    for (Long ts : getTs(key))
    {
      if (original)
        ts -= velDur * 60 * 1000;

      Long tm = ts / (long) div;
      Long ms = tm * (long) div;
      Integer v = ll.get(ms);

      if (v == null)
        ll.put(ms, 1);
      else
        ll.put(ms, v + 1);
    }

    return ll;
  }

  // count records and display every Nth record
  abstract public int count(int n);

  public static int count(String db, int freq)
  {
    Vel vel = context(db);

    return vel != null ? vel.count(freq) : null;
  }

  public int purgeDb()
  {
    return purgeDb(null);
  }

  public synchronized int purgeDb(String until)
  {
    return purgeDb(getCurr(until));
  }

  abstract protected int purgeDb(int now);

  public static int purge(String db)
  {
     return purge(db, null);
  }

  public synchronized static int purge(String db, String until)
  {
    Vel vel = context(db);

    return vel != null ? vel.purgeDb(until) : null;
  }

  abstract public int recCount();

  public static int recCount(String db)
  {
    Vel vel = context(db);

    return vel != null ? vel.recCount() : 0;
  }

  public String stats()
  {
    Runtime rt = Runtime.getRuntime();

    return (rt.totalMemory() / 1024 / 1024) + "|" + (rt.freeMemory() / 1024 / 1024);
  }

  public static String stats(String db)
  {
    Vel vel = context(db);

    return vel != null ? vel.stats() : null;
  }

  protected static Vel getVel(String type, String nm)
  {
    return context(nm, type);
  }

  public Vel copy(String type, String nm)
  {
    return copy(type, nm, false);
  }

  public Vel merge(String type, String nm)
  {
    return copy(type, nm, true);
  }

  // Copy from one implementation to another
  abstract Vel copy(String type, String nm, boolean merge);

  protected final Base merge(long lkey, Base base, int max)
  {
    Base c = get(lkey);

    if (c == null)   // Should be most likely scenario
      set(lkey, base);
    else if (c.compareTo(base) != 0)
      set(lkey, base = velMerge(c, base, max));

    return base;
  }

  public final static Base merge(String rem, int max)
  {
    String [] rems = rem.split(":");

    Vel vel = getVel(rems[0], rems[1]);

    return vel.merge(rems, max);
  }

  public final Base merge(String[] rems, int max)
  {
    long lkey = Long.parseLong(rems[2]);
    Base c = get(lkey);
    Base base = null;


    if (rems[3].contains(","))
    {
      String[] its = rems[3].split(",");

      Item[] is = new Item[its.length];

      for (int i = 0; i != its.length; i++)
      {
        String[] it = its[i].split("|");

        if (it.length == 2)
          is[i] = new Item(Integer.parseInt(its[0]), Integer.parseInt(its[1]));
        else
          is[i] = new Item(Integer.parseInt(its[0]));
      }

      base = new Items(is);
    }
    else
    {
      String[] it = rems[3].split("\\|");

      if (it.length == 2)
        base = new Item(Integer.parseInt(it[0]), Integer.parseInt(it[1]));
      else
        base = new Item(Integer.parseInt(it[0]));
    }

    if (c == null)   // Should be most likely scenario
      set(lkey, base);
    else if (c.compareTo(base) != 0)
      set(lkey, base = velMerge(c, base, max));

    return base;
  }

  // Merge two values together
  private final Base velMerge(Base c, Base base, int max)
  {
    Items newEnds = null;

    if (c instanceof Item)
    {
      if (base instanceof Item)
      {
        newEnds = new Items(new Item[2]);

        newEnds.items[0] = (Item) c;
        newEnds.items[1] = (Item) base;
      }
      else
      {
        newEnds = new Items(new Item[((Items) base).items.length + 1]);

        int j = 0;

        for (Item i : ((Items) base).items)
          newEnds.items[j++] = i;

        newEnds.items[j] = (Item) c;
      }
    }
    else if (base instanceof Item)
    {
      newEnds = new Items(new Item[((Items) c).items.length + 1]);

      int j = 0;

      for (Item i : ((Items) c).items)
        newEnds.items[j++] = i;

      newEnds.items[j] = (Item) base;
    }
    else
    {
      int l = ((Items) c).items.length + ((Items) base).items.length;

      newEnds = new Items(new Item[l]);

      int j = 0;

      for (Item i : ((Items) c).items)
        newEnds.items[j++] = i;
      for (Item i : ((Items) base).items)
        newEnds.items[j++] = i;
    }

    // sort it to be insertion order independent
    Arrays.sort(newEnds.items);

    int le = 0;
    int actual = 0;

    // null out inappicable entries
    for (int i = 0; i != newEnds.items.length; i++)
    {
      Item e = newEnds.items[i];

      // when no value or duplicate
      if (e.ts == 0 || e.ts == le)
        newEnds.items[i] = null;
      else
        actual++;

      le = e.ts;
    }

    // final string
    Item[] endList = new Item[actual];
    int j = 0;

    // Get out what we want and reverse!
    for (int i = newEnds.items.length - 1; i >= 0; i--)
    {
      Item e = newEnds.items[i];

      if (e != null)
        endList[j++] = e;
    }

    base = actual == 1 ? endList[0] : new Items(endList);

    Items e = (Items) base;

    // ensure max length
    if (e.items.length > max)
    {
      Item[] n = new Item[max]; 

      for (int i = 0; i != max; i++)
        n[i] = e.items[i];

      e.items = n;
    }

    return base;
  }

  private final Base velGet(Base c, int dur, String now, int max)
  {
    return velGet(c, 0, dur, getCurr(now), max);
  }

  private final Base velGet(Base c, int dur, Date now, int max)
  {
    return velGet(c, 0, dur, (int) (now.getTime() / 60 / 1000), max);
  }

  private final Base velGet(Base c, int dur, int curr, int max)
  {
    return velGet(c, 0, dur, curr, max);
  }

  private final Base velGet(Base c, double val, int dur, String now, int max)
  {
    return velGet(c, toFix(val), dur, getCurr(now), max);
  }

  private final Base velGet(Base c, double val, int dur, Date now, int max)
  {
    return velGet(c, toFix(val), dur, (int) (now.getTime() / 60 / 1000), max);
  }

  private final Base velGet(Base c, double val, int dur, int curr, int max)
  {
    return velGet(c, toFix(val), dur, curr, max);
  }

  private final Base velGet(Base c, int val, int dur, String now, int max)
  {
    return velGet(c, val, dur, getCurr(now), max);
  }

  private final Base velGet(Base c, int val, int dur, Date now, int max)
  {
    return velGet(c, val, dur, (int) (now.getTime() / 60 / 1000), max);
  }

  private final Base velGet(Base c, int val, int dur, int curr, int max)
  {
    int expire = 0;

    if (dur >= 0)   // Only false if query only
    {
      if (saveParas && velDur != dur && dur > 0)
      {
        velDur = dur;

        saveProp();
      }

      expire = curr + ((dur == 0) ? velDur : dur);
    }

    Base base = null;

    if (c == null)
      base = new Item(expire, val);
    else  // Not New - occasionally
    {
      Item[] newEnds = null;

      if (c instanceof Item)
      {
        Item orig = (Item) c;

        if (expire == orig.ts)
        {
          orig.item += val;

          base = c;
        }
        else if (max == 1)
        {
          orig.ts = expire;
          orig.item = val;

          base = c;
        }
        else
        {
          if (curr > orig.ts)
          {
            orig.ts = expire;
            orig.item = val;

            base = orig;
          }
          else
          {
            newEnds = new Item[2];
            newEnds[0] = new Item(expire, val);
            newEnds[1] = orig;

            base = new Items(newEnds);
          }
        }
      }
      else
      {
        Items orig = (Items) c;
        Item[] ends = orig.items;

        if (expire == ends[0].ts)
        {
          ends[0].item += val;

          if (curr > ends[1].ts)
            base = ends[0];
          else
            base = orig;
        }
        else
        {
          int i;

          for (i = 0; i != ends.length; i++)
          {
            if (ends[i] != null && (ends[i].ts == 0 || ends[i].ts < curr))
              break;
          }

          if (i >= max)
          {
            newEnds = ends;   // reuse

            // Overlapping... So copy backwards.
            for (int j = i - 1; j > 0; j--)
              newEnds[j] = newEnds[j - 1];
            /*
            newEnds = new Item[i];

            System.arraycopy(ends, 0, newEnds, 1, newEnds.length - 1);
            */
          }
          else
          {
            newEnds = new Item[i + 1];

            System.arraycopy(ends, 0, newEnds, 1, newEnds.length - 1);
          }

          if (newEnds.length > 1)
          {
            newEnds[0] = new Item(expire, val);

            orig.items = newEnds;

            base = orig;
          }
          else
            base = new Item(expire, val);
        }
      }
    }

    return base;
  }

  abstract Base removeKey(long key);
  abstract Base get(long key);

  public int velCount(String key)
  {
    return velCount(key, null, 0, getCurr(null));
  }

  public int velCount(String key, String dtOrRe)
  {
    int dre = getCurr(dtOrRe);

    if (dre > 0)
      return velCount(key, null, 0, dre);
    else
      return velCount(key, dtOrRe, 0, getCurr(null));
  }

  public int velCount(String key, Date now)
  {
    return velCount(key, null, 0, (int) (now.getTime() / 60 / 1000));
  }

  public int velCount(String key, String re, String dt)
  {
    return velCount(key, re, 0, getCurr(dt));
  }

  public int velCount(String key, String re, Date now)
  {
    return velCount(key, re, 0, (int) (now.getTime() / 60 / 1000));
  }

  public int velCount(String key, int dur)
  {
    return velCount(key, null, dur, getCurr(null));
  }

  public int velCount(String key, int dur, String dtOrRe)
  {
    int dre = getCurr(dtOrRe);

    if (dre > 0)
      return velCount(key, null, dur, dre);
    else
      return velCount(key, dtOrRe, dur, getCurr(null));
  }

  public int velCount(String key, int dur, Date now)
  {
    return velCount(key, null, dur, (int) (now.getTime() / 60 / 1000));
  }

  public int velCount(String key, int dur, int dt)
  {
    return velCount(key, null, dur, dt);
  }

  public int velCount(String key, String re, int dur)
  {
    return velCount(key, re, dur, getCurr(null));
  }

  public int velCount(String key, String re, int dur, String dt)
  {
    return velCount(key, re, dur, getCurr(dt));
  }

  public int velCount(String key, String re, int dur, Date now)
  {
    return velCount(key, re, dur, (int) (now.getTime() / 60 / 1000));
  }

  public int velCount(String key, String re, int dur, int dt)
  {
    long lkey = vhash(re != null && key.matches(re) ? re : key);
    Base base = get(lkey);
    int count = 0;

    try
    {
      base = velGet(base, 1, dur, dt, max);

      if (base instanceof Item)
      {
        count = ((Item) base).item;
      }
      else  // must be more than 1 item
      {
        for (Item i : ((Items) base).items)
          count += i.item;
      }

      set(lkey, base);
    }
    catch (Exception ex)
    {
      count = 0;

      log("velCount failed", ex);
    }

    return count;
  }

  public int velValue(String key, int v)
  {
    return velValue(key, null, v, 0, getCurr(null));
  }

  public int velValue(String key, String dtOrRe, int v)
  {
    int dre = getCurr(dtOrRe);

    if (dre > 0)
      return velValue(key, null, v, 0, dre);
    else
      return velValue(key, dtOrRe, v, 0, getCurr(null));
  }

  public int velValue(String key, int v, Date now)
  {
    return velValue(key, null, v, 0, (int) (now.getTime() / 60 / 1000));
  }

  public int velValue(String key, String re, int v, String dt)
  {
    return velValue(key, re, v, 0, getCurr(dt));
  }

  public int velValue(String key, String re, int v, Date now)
  {
    return velValue(key, re, v, 0, (int) (now.getTime() / 60 / 1000));
  }

  public int velValue(String key, int v, int dur)
  {
    return velValue(key, null, v, dur, getCurr(null));
  }

  public int velValue(String key, int v, int dur, String dt)
  {
    return velValue(key, null, v, dur, getCurr(dt));
  }

  public int velValue(String key, int v, int dur, Date now)
  {
    return velValue(key, null, v, dur, (int) (now.getTime() / 60 / 1000));
  }

  public int velValue(String key, int v, int dur, int dt)
  {
    return velValue(key, null, v, dur, dt);
  }

  public int velValue(String key, String re, int v, int dur)
  {
    return velValue(key, re, v, dur, getCurr(null));
  }

  public int velValue(String key, String re, int v, int dur, String dt)
  {
    return velValue(key, re, v, dur, getCurr(dt));
  }

  public int velValue(String key, String re, int v, int dur, Date now)
  {
    return velValue(key, re, v, dur, (int) (now.getTime() / 60 / 1000));
  }

  public int velValue(String key, String re, int v, int dur, int dt)
  {
    long lkey = vhash(re != null && key.matches(re) ? re : key);
    Base base = get(lkey);
    int val = 0;

    try
    {
      base = velGet(base, v, dur, dt, max);

      if (base instanceof Item)
        val = ((Item) base).item;
      else
      {
        for (Item i : ((Items) base).items)
          val += i.item;
      }

      set(lkey, base);
    }
    catch (Exception ex)
    {
ex.printStackTrace();
      val = 0;

      log("velValue failed", ex);
    }

    return val;
  }

  public double velValue(String key, double v, int dur)
  {
    return velValue(key, null, v, dur, getCurr(null));
  }

  public double velValue(String key, double v, int dur, String dt)
  {
    return velValue(key, null, v, dur, getCurr(dt));
  }

  public double velValue(String key, double v, int dur, Date now)
  {
    return velValue(key, null, v, dur, (int) (now.getTime() / 60 / 1000));
  }

  public double velValue(String key, double v, int dur, int dt)
  {
    return velValue(key, null, v, dur, dt);
  }

  public double velValue(String key, String re, double v, int dur)
  {
    return velValue(key, re, v, dur, getCurr(null));
  }

  public double velValue(String key, String re, double v, int dur, String dt)
  {
    return velValue(key, re, v, dur, getCurr(dt));
  }

  public double velValue(String key, String re, double v, int dur, Date now)
  {
    return velValue(key, re, v, dur, (int) (now.getTime() / 60 / 1000));
  }

  public double velValue(String key, String re, double v, int dur, int dt)
  {
    long lkey = vhash(re != null && key.matches(re) ? re : key);
    Base base = get(lkey);
    double val = 0.0;

    try
    {
      base = velGet(base, v, dur, dt, max);

      if (base instanceof Item)
        val = ((Item) base).item;
      else
      {
        int d = 0;

        for (Item i : ((Items) base).items)
          d += i.item;

        val = fromFix(d);
      }

      set(lkey, base);
    }
    catch (Exception ex)
    {
      val = 0.0;

      log("velValue failed", ex);
    }

    return val;
  }

  public boolean velOr(String key, int hitMax, double v, double valMax, int dur)
  {
    return velOr(key, null, hitMax, v, valMax, dur, getCurr(null));
  }

  public boolean velOr(String key, int hitMax, double v, double valMax, int dur, String dt)
  {
    return velOr(key, null, hitMax, v, valMax, dur, getCurr(dt));
  }

  public boolean velOr(String key, int hitMax, double v, double valMax, int dur, Date now)
  {
    return velOr(key, null, hitMax, v, valMax, dur, (int) (now.getTime() / 60 / 1000));
  }

  public boolean velOr(String key, int hitMax, double v, double valMax, int dur, int dt)
  {
    return velOr(key, null, hitMax, v, valMax, dur, dt);
  }

  public boolean velOr(String key, String re, int hitMax, double v, double valMax, int dur)
  {
    return velOr(key, re, hitMax, v, valMax, dur, getCurr(null));
  }

  public boolean velOr(String key, String re, int hitMax, double v, double valMax, int dur, String dt)
  {
    return velOr(key, re, hitMax, v, valMax, dur, getCurr(dt));
  }

  public boolean velOr(String key, String re, int hitMax, double v, double valMax, int dur, Date now)
  {
    return velOr(key, re, hitMax, v, valMax, dur, (int) (now.getTime() / 60 / 1000));
  }

  /**
   *  Call a Velocity object and return a boolean to indicate if it
   *  has been triggered or not.
   *  Static version that uses global Velocity Persistent tree
   *
   * @param  key  The key by which the row is indexed
   * @param  hitMax     The hit max is the Velocity count threshold over
   *       which a Velocity match is deemed to have occurred
   * @param  v      The value supplied by this call (to add to get
   *            valMax)
   * @param  valMax     The val max is the Velocity value threshold over
   *       which a Velocity match is deemed to have occurred
   * @param  dur      The Velocity duration is the time interval
   *       (measured in hours) over which the moving window
   *       of Velocity operates. If a hit on the same Velocity
   *       component(s) occurs within the window then the count
   *       or value will be incremented. If it is not within
   *       the window then the count will be reset to 1.
   *
   * @return        Has the item occured the number of times OR
   *            value times in the duration?
   */
  public boolean velOr(String key, String re, int hitMax, double v, double valMax, int dur, int dt)
  {
    long lkey = vhash(re != null && key.matches(re) ? re : key);
    Base base = get(lkey);
    boolean good = true;

    try
    {
      base = velGet(base, v, dur, dt, max);

      if (hitMax == -1)
        hitMax = 99999999;
      else
        hitMax = (hitMax == 0) ? hitMax : hitMax;

      valMax = (valMax == 0.0) ? valMax : valMax;

      if (base instanceof Item)
      {
        if ((1 >= hitMax && hitMax > 0) ||
            (fromFix(((Item) base).item) >= valMax && valMax > 0.0))
          good = false;
      }
      else
      {
        int d = 0;
        Items ents = (Items) base;

        for (Item e : ents.items)
          d += e.item;    // Sum of values

        if ((ents.items.length >= hitMax && hitMax > 0) ||
            (fromFix(d) >= valMax && valMax > 0.0))
          good = false;
      }

      set(lkey, base);
    }
    catch (Exception ex)
    {
      good = true;

      log("velOr failed", ex);

      close();
    }

    return good;
  }

  public boolean velAnd(String key, int hitMax, double v, double valMax, int dur)
  {
    return velAnd(key, null, hitMax, v, valMax, dur, getCurr(null));
  }

  public boolean velAnd(String key, int hitMax, double v, double valMax, int dur, String dt)
  {
    return velAnd(key, null, hitMax, v, valMax, dur, getCurr(dt));
  }

  public boolean velAnd(String key, int hitMax, double v, double valMax, int dur, Date now)
  {
    return velAnd(key, null, hitMax, v, valMax, dur, (int) (now.getTime() / 60 / 1000));
  }

  public boolean velAnd(String key, int hitMax, double v, double valMax, int dur, int dt)
  {
    return velAnd(key, null, hitMax, v, valMax, dur, dt);
  } 

  public boolean velAnd(String key, String re, int hitMax, double v, double valMax, int dur)
  {
    return velAnd(key, re, hitMax, v, valMax, dur, getCurr(null));
  }

  public boolean velAnd(String key, String re, int hitMax, double v, double valMax, int dur, String dt)
  {
    return velAnd(key, re, hitMax, v, valMax, dur, getCurr(dt));
  }

  public boolean velAnd(String key, String re, int hitMax, double v, double valMax, int dur, Date now)
  {
    return velAnd(key, re, hitMax, v, valMax, dur, (int) (now.getTime() / 60 / 1000));
  }

  /**
   *  Call a Velocity object and return a boolean to indicate if it
   *  has been triggered or not.
   *  Static version that uses global Velocity Persistent tree
   *
   * @param  key  The key by which the row is indexed
   * @param  hitMax     The hit max is the Velocity count threshold over
   *       which a Velocity match is deemed to have occurred
   * @param  v      The value supplied by this call (to add to get
   *            valMax)
   * @param  valMax     The val max is the Velocity value threshold over
   *       which a Velocity match is deemed to have occurred
   * @param  dur      The Velocity duration is the time interval
   *       (measured in hours) over which the moving window
   *       of Velocity operates. If a hit on the same Velocity
   *       component(s) occurs within the window then the count
   *       or value will be incremented. If it is not within
   *       the window then the count will be reset to 1.
   *
   * @return        Has the item occured the number of times AND
   *            value times in the duration?
   */
  public boolean velAnd(String key, String re, int hitMax, double v, double valMax, int dur, int dt)
  {
    long lkey = vhash(re != null && key.matches(re) ? re : key);
    Base base = get(lkey);
    boolean good = true;

    try
    {
      base = velGet(base, v, dur, dt, max);

      if (hitMax == -1)
        hitMax = 99999999;
      else
        hitMax = (hitMax == 0) ? hitMax : hitMax;

      valMax = (valMax == 0.0) ? valMax : valMax;

      if (base instanceof Item)
      {
        if ((1 >= hitMax && hitMax > 0) &&
            (fromFix(((Item) base).item) >= valMax && valMax > 0.0))
          good = false;
      }
      else
      {
        int d = 0;
        Items ents = (Items) base;

        for (Item e : ents.items)
          d += e.item;    // Sum of values

        if ((ents.items.length >= hitMax && hitMax > 0) &&
            (fromFix(d) >= valMax && valMax > 0.0))
          good = false;
      }

      set(lkey, base);
    }
    catch (Exception ex)
    {
      good = true;

      log("velAnd failed", ex);

      close();
    }

    return good;
  }

  public int getCount(String key)
  {
    return getCount(key, null, getCurr(null));
  }

  public int getCount(String key, String dtre)
  {
    if (dtre.length() == 19 && dtre.contains("-") && dtre.contains(":"))
      return getCount(key, null, getCurr(dtre));
    else
      return getCount(key, dtre, getCurr(null));
  }

  public int getCount(String key, Date now)
  {
    return getCount(key, null, (int) (now.getTime() / 60 / 1000));
  }

  public int getCount(String key, int now)
  {
    return getCount(key, null, now);
  }

  public int getCount(String key, String re, String dt)
  {
    return getCount(key, re, getCurr(dt));
  }

  public int getCount(String key, String re, Date now)
  {
    return getCount(key, re, (int) (now.getTime() / 60 / 1000));
  }

  public int getCount(String key, String re, int now)
  {
    long lkey = vhash(re != null && key.matches(re) ? re : key);
    int hits = 0;

    try
    {
      Base base = get(lkey);

      if (base == null)
        return 0;
      else if (base instanceof Item)
        hits = 1;
      else
        hits = ((Items) base).items.length;
    }
    catch (Exception ex)
    {
ex.printStackTrace();
      hits = 0;

      log("getCount failed", ex);

      //close();
    }

    return hits;
  }

  public double getValue(String key)
  {
    return getValue(key, null, getCurr(null));
  }

  public double getValue(String key, String dtre)
  {
    if (dtre.length() == 19 && dtre.contains("-") && dtre.contains(":"))
      return getValue(key, null, getCurr(dtre));
    else
      return getValue(key, dtre, getCurr(null));
  }

  public double getValue(String key, Date now)
  {
    return getValue(key, null, (int) (now.getTime() / 60 / 1000));
  }

  public double getValue(String key, int now)
  {
    return getValue(key, null, now);
  }

  public double getValue(String key, String re, String dt)
  {
    return getValue(key, re, getCurr(dt));
  }

  public double getValue(String key, String re, Date now)
  {
    return getValue(key, re, (int) (now.getTime() / 60 / 1000));
  }

  public double getValue(String key, String re, int now)
  {
    long lkey = vhash(re != null && key.matches(re) ? re : key);
    int d = 0;

    try
    {
      Base base = get(lkey);

      if (base instanceof Item)
        d = ((Item) base).item;
      else
      {
        Items ents = (Items) base;

        for (Item e : ents.items)
          d += e.item;    // Sum of values
      }
    }
    catch (Exception ex)
    {
      d = 0;

      log("getValue failed", ex);

      //close();
    }

    return d;
  }

  public int velDiff(String re, String val, int dur)
  {
    return velChange("", re, val, null, dur, getCurr(null));
  }

  public int velDiff(String re, String val, int dur, String dt)
  {
    return velChange("", re, val, null, dur, getCurr(dt));
  }

  public int velDiff(String re, String val, int dur, Date now)
  {
    return velChange("", re, val, null, dur, (int) (now.getTime() / 60 / 1000));
  }

  public int velDiff(String re, String val, int dur, int now)
  {
    return velChange("", re, val, null, dur, now);
  }

  public int velChange(String key, String re, String val, int dur)
  {
    return velChange(key, re, val, null, dur, getCurr(null));
  }

  public int velChange(String key, String re, String val, int dur, String dt)
  {
    return velChange(key, re, val, null, dur, getCurr(dt));
  }

  public int velChange(String key, String re, String val, int dur, Date now)
  {
    return velChange(key, re, val, null, dur, (int) (now.getTime() / 60 / 1000));
  }

  public int velChange(String key, String re, String val, int dur, int now)
  {
    return velChange(key, re, val, null, dur, now);
  }

  public int velChange(String key, String re, String val, String vre, int dur)
  {
    return velChange(key, re, val, vre, dur, getCurr(null));
  }

  public int velChange(String key, String re, String val, String vre, int dur, String dt)
  {
    return velChange(key, re, val, vre, dur, getCurr(dt));
  }

  public int velChange(String key, String re, String val, String vre, int dur, Date now)
  {
    return velChange(key, re, val, vre, dur, (int) (now.getTime() / 60 / 1000));
  }

  /**
   *  Call a change and return a integer of the number of differences found
   *  to the current value
   *  Static version that uses global change Persistent tree
   *
   * @param  key  The key by which the row is indexed
   * @param  val  The item / value stored under thay key
   * @param  dur      The velocity duration is the time interval
   *       (measured in hours) over which the moving window
   *       of velocity operates. If a hit on the same velocity
   *       component(s) occurs within the window then the count
   *       or value will be incremented. If it is not within
   *       the window then the count will be reset to 1.
   *
   * @return      number of occurrences
   */
  public int velChange(String key, String re, String val, String vre, int dur, int now)
  {
    String pat = null;

    if (re != null && key.matches(re))
    {
      if (vre != null && val.matches(vre))
        pat = re + "|" + vre;
      else
        pat = re + "|" + val;
    }
    else if (vre != null && val.matches(vre))
      pat = key + "|" + vre;
    else
      pat = key + "|" + val;

    long lkey = vhash(pat);
    int res = 0;

    try
    {
      Base base = velGet(get(lkey), dur, now, max);

      if (base instanceof Item)
        res = 1;
      else
      {
        Items ents = (Items) base;
        int[] its = new int[ents.items.length];

        for (int i = 0; i != its.length; i++)
          its[i] = ents.items[i].item;

        Arrays.sort(its);

        int last = 0;

        for (int i : its)
        {
          if (last == 0 || last != i)
            res++;

          last = i;
        }
      }

      set(lkey, base);
    }
    catch (Exception ex)
    {
      res = 0;

      log("change failed", ex);

      //close();
    }

    return res;
  }

  public int getDiff(String re, String val)
  {
    return getChange("", re, val, null, getCurr(null));
  }

  public int getDiff(String re, String val, String dt)
  {
    return getChange("", re, val, null, getCurr(dt));
  }

  public int getDiff(String re, String val, Date now)
  {
    return getChange("", re, val, null, (int) (now.getTime() / 60 / 1000));
  }

  public int getDiff(String re, String val, int now)
  {
    return getChange("", re, val, null, now);
  }

  public int getChange(String key, String re, String val, String vre)
  {
    return getChange(key, re, val, vre, getCurr(null));
  }

  public int getChange(String key, String re, String val, String vre, String dt)
  {
    return getChange(key, re, val, vre, getCurr(dt));
  }

  public int getChange(String key, String re, String val, String vre, Date now)
  {
    return getChange(key, re, val, vre, (int) (now.getTime() / 60 / 1000));
  }

  public int getChange(String key, String re, String val, String vre, int now)
  {
    String pat = null;

    if (re != null && key.matches(re))
    {
      if (vre != null && val.matches(vre))
        pat = re + "|" + vre;
      else
        pat = re + "|" + val;
    }
    else if (vre != null && val.matches(vre))
      pat = key + "|" + vre;
    else
      pat = key + "|" + val;

    long lkey = vhash(pat);
    int res = 0;

    try
    {
      Base base = get(lkey);

      if (base == null)
        res = 0;
      else if (base instanceof Item)
        res = 1;
      else
      {
        Items ents = (Items) base;
        int[] its = new int[ents.items.length];

        for (int i = 0; i != its.length; i++)
          its[i] = ents.items[i].item;

        Arrays.sort(its);

        int last = 0;

        for (int i : its)
        {
          if (last == 0 || last != i)
            res++;

          last = i;
        }
      }
    }
    catch (Exception ex)
    {
//ex.printStackTrace();
      res = 0;

      log("getChange failed", ex);

      //close();
    }

    return res;
  }

  public String removeKey(String key)
  {
    Object base = removeKey(vhash(key));

    return base == null ? null : base.toString();
  }

  static int toFix(double d)
  {
    return (int) ((d * 100.0) + 0.5);
  }

  static double fromFix(int i)
  {
    return (double) i / 100.0;
  }

  public static int getCurr()
  {
    return getCurr(null);
  }

  public static int getCurr(String now)
  {
    Date dt = (now == null || now.isEmpty()) ? new Date() : parseDt(now);

    return (int) (dt.getTime() / 60 / 1000);
  }

  private static Date parseDt(String format)
  {
    return parseDt(format, "GMT");
  }

  private static Date parseDt(String format, String tz)
  {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    Calendar cal = Calendar.getInstance();

    try
    {
      cal.setTime(sdf.parse(format + " " + tz, new ParsePosition(0)));
    }
    catch (NullPointerException ex)
    {
      // Ignore

      return new Date(0L);
    }

    return cal.getTime();
  }

  public static String getRevision()
  {
    return "$Revision: 1.1 $";
  }

  public static void startMem(long startTime, long before)
  {
    System.err.printf("Start : %8.2f MB%n", (before / 1024.0 / 1024.0));
  }

  public static long showMem(long startTime, long before, Runtime rt)
  {
    long after = rt.totalMemory() - rt.freeMemory();
    long duration = System.currentTimeMillis() - startTime;

    System.err.printf("%8.2f MB, Duration : %d ms%n", (after / 1024.0 / 1024.0), duration);

    return duration;
  }

  public static int getTs(long val)
  {
    return (int) (val >> 32);
  }

  protected static int getItem(long val)
  {
    return (int) (val & 0xFFFFFFFFL);
  }

  protected Item toItem(long val)
  {
    return new Item(getTs(val), getItem(val));
  }

  protected static long pack(int x, int y)
  {
    return (((long) x) << 32) | (y & 0xFFFFFFFFL);
  }

  private static final int PRIME1 = -1640531535;
  private static final int PRIME2 = -2048144777;
  private static final int PRIME3 = -1028477379;
  private static final int PRIME4 = 668265263;
  private static final int PRIME5 = 374761393;

  public static final int hash(byte[] buf, int seed)
  {
    return hash(buf, 0, buf.length, seed);
  }

  public static final int hash(byte[] buf, int off, int len, int seed)
  {
    final int end = off + len;
    int h32;

    if (len >= 16)
    {
      final int limit = end - 16;
      int v1 = seed + PRIME1 + PRIME2;
      int v2 = seed + PRIME2;
      int v3 = seed + 0;
      int v4 = seed - PRIME1;

      do
      {
        v1 += readIntLE(buf, off) * PRIME2;
        v1 = rotateLeft(v1, 13);
        v1 *= PRIME1;
        off += 4;

        v2 += readIntLE(buf, off) * PRIME2;
        v2 = rotateLeft(v2, 13);
        v2 *= PRIME1;
        off += 4;

        v3 += readIntLE(buf, off) * PRIME2;
        v3 = rotateLeft(v3, 13);
        v3 *= PRIME1;
        off += 4;

        v4 += readIntLE(buf, off) * PRIME2;
        v4 = rotateLeft(v4, 13);
        v4 *= PRIME1;
        off += 4;
      }
      while (off <= limit);

      h32 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
    }
    else 
      h32 = seed + PRIME5;

    h32 += len;

    while (off <= end - 4)
    {
      h32 += readIntLE(buf, off) * PRIME3;
      h32 = rotateLeft(h32, 17) * PRIME4;
      off += 4;
    }

    while (off < end)
    {
      h32 += (buf[off] & 0xFF) * PRIME5;
      h32 = rotateLeft(h32, 11) * PRIME1;
      ++off;
    }

    h32 ^= h32 >>> 15;
    h32 *= PRIME2;
    h32 ^= h32 >>> 13;
    h32 *= PRIME3;
    h32 ^= h32 >>> 16;

    return h32;
  }

  private static int readIntLE(byte[] buf, int i)
  {
    return (buf[i] & 0xFF) | ((buf[i+1] & 0xFF) << 8) | ((buf[i+2] & 0xFF) << 16) | ((buf[i+3] & 0xFF) << 24);
  }

  /*
  public static void main(String... args)
  {
  }
  */
}
