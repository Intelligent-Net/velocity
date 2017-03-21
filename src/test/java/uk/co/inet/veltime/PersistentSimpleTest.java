package uk.co.inet.veltime;

import java.io.*;
import java.nio.file.*;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class PersistentSimpleTest
{
  private static void delete(File file)
    throws IOException
  {
    if (file.isDirectory())
    {
      if (file.list().length == 0)
        file.delete();
      else
      {
        for (String temp : file.list())
          delete(new File(file, temp));

        if (file.list().length == 0)
          file.delete();
      }
    }
    else
      file.delete();
  }

  @Before
  public void setup()
  {
    try
    {
      delete(new File("veldb"));
    }
    catch (IOException x)
    {
      System.err.println(x);
    }
  }

  /**
   * Simple count test
   */
  @Test
  public void testCountBasic()
  {
    int cnt = 0;
    
    cnt = PersistentVel.context("pvel").velCount("persisttest1", 1, "2017-01-01 12:12:00");

    assertEquals(1, cnt);
    
    cnt = PersistentVel.context("pvel").velCount("persisttest1", 1, "2017-01-01 12:12:30");

    assertEquals(2, cnt);
 
    cnt = PersistentVel.context("pvel").velCount("persisttest1", 1, "2017-01-01 12:13:00");

    assertEquals(3, cnt);
 
    cnt = PersistentVel.context("pvel").velCount("persisttest1", 1, "2017-01-01 12:13:30");

    assertEquals(4, cnt);
 
    cnt = PersistentVel.context("pvel").velCount("persisttest1", 1, "2017-01-01 12:13:45");

    assertEquals(5, cnt);
 
    cnt = PersistentVel.context("pvel").velCount("persisttest1", 1, "2017-01-01 12:14:01");

    assertEquals(4, cnt);
 
    cnt = PersistentVel.context("pvel").velCount("persisttest1", 1, "2017-01-01 12:17:00");

    assertEquals(1, cnt);

    cnt = PersistentVel.context("pvel").velCount("persisttest1", 1, "2017-01-01 12:20:00");

    assertEquals(1, cnt);
  }

  /**
   * Simple value test
   */
  @Test
  public void testValueBasic()
  {
    int cnt = 0;
    
    cnt = PersistentVel.context("pvel").velValue("persisttest2", 1, 1, "2017-01-01 12:12:00");

    assertEquals(1, cnt);
    
    cnt = PersistentVel.context("pvel").velValue("persisttest2", 1, 1, "2017-01-01 12:12:30");

    assertEquals(2, cnt);
 
    cnt = PersistentVel.context("pvel").velValue("persisttest2", 1, 1, "2017-01-01 12:13:00");

    assertEquals(3, cnt);
 
    cnt = PersistentVel.context("pvel").velValue("persisttest2", 1, 1, "2017-01-01 12:13:30");

    assertEquals(4, cnt);
 
    cnt = PersistentVel.context("pvel").velValue("persisttest2", 1, 1, "2017-01-01 12:13:45");

    assertEquals(5, cnt);
 
    cnt = PersistentVel.context("pvel").velValue("persisttest2", 1, 1, "2017-01-01 12:14:01");

    assertEquals(4, cnt);
 
    cnt = PersistentVel.context("pvel").velValue("persisttest2", 1, 1, "2017-01-01 12:17:00");

    assertEquals(1, cnt);
 
    cnt = PersistentVel.context("pvel").velValue("persisttest2", 2, 1, "2017-01-01 12:17:30");

    assertEquals(3, cnt);

    cnt = PersistentVel.context("pvel").velValue("persisttest2", 2, 1, "2017-01-01 12:20:00");

    assertEquals(2, cnt);
  }

  /**
   * Simple value test
   */
  @Test
  public void testCountHits()
  {
    int origMax = MemoryVel.context("pvel").getMax();
    int max = origMax;

    for (int m = 0; m != 3; m++)
    {
      int cnt = 0;

      for (int i = 0; i != max; i++)
      {
        cnt = MemoryVel.context("pvel").velCount("perhittest" + m, 1, "2017-01-01 12:12:0" + i);

        assertEquals(i + 1, cnt);
      }

      cnt = MemoryVel.context("pvel").velCount("perhittest" + m, 1, "2017-01-01 12:12:" + String.format("%2.2s", max));

      assertNotEquals(max, cnt);

      switch(m)
      {
        case 0 : max = origMax * 2; break;
        case 1 : max = origMax / 2; break;
      }

      MemoryVel.context("pvel").setMax(max);
    }
  }
}
