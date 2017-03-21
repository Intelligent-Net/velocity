package uk.co.inet.veltime;

import java.util.Date;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class MemorySimpleTest
{
  @Before
  public void setup()
  {
  }

  /**
   * Simple count test
   */
  @Test
  public void testCountBasic()
  {
    int cnt = 0;
    
    cnt = MemoryVel.context("mvel").velCount("memtest1", 1, "2017-01-01 12:12:00");

    assertEquals(1, cnt);
    
    cnt = MemoryVel.context("mvel").velCount("memtest1", 1, "2017-01-01 12:12:30");

    assertEquals(2, cnt);
 
    cnt = MemoryVel.context("mvel").velCount("memtest1", 1, "2017-01-01 12:13:00");

    assertEquals(3, cnt);
 
    cnt = MemoryVel.context("mvel").velCount("memtest1", 1, "2017-01-01 12:13:30");

    assertEquals(4, cnt);
 
    cnt = MemoryVel.context("mvel").velCount("memtest1", 1, "2017-01-01 12:13:45");

    assertEquals(5, cnt);
 
    cnt = MemoryVel.context("mvel").velCount("memtest1", 1, "2017-01-01 12:14:01");

    assertEquals(4, cnt);
 
    cnt = MemoryVel.context("mvel").velCount("memtest1", 1, "2017-01-01 12:17:00");

    assertEquals(1, cnt);
  }

  /**
   * Many count Test
   */
  @Test
  public void testManyCount()
  {
    int cnt = 0;
    Vel vel = MemoryVel.context("mmvel");
    long dt = (System.currentTimeMillis() / (60 * 1000)) * (60 * 1000);
    int ms = 100;
    int n = 0;

    vel.setMax(2);
    
    for (int i = 0; i != 600; i++)
    {
      cnt = vel.velCount("memmanytest1", 1, new Date(dt += ms));

      if (i >= 1000)
        n = 1000;
      else
        n = i + 1;
        
      assertEquals(n, cnt);
    }
System.err.println("cnt = " + cnt);
  }

  /**
   * Simple value test
   */
  @Test
  public void testValueBasic()
  {
    int cnt = 0;
    
    cnt = MemoryVel.context("mvel").velValue("memtest2", 1, 1, "2017-01-01 12:12:00");

    assertEquals(1, cnt);
    
    cnt = MemoryVel.context("mvel").velValue("memtest2", 1, 1, "2017-01-01 12:12:30");

    assertEquals(2, cnt);
 
    cnt = MemoryVel.context("mvel").velValue("memtest2", 1, 1, "2017-01-01 12:13:00");

    assertEquals(3, cnt);
 
    cnt = MemoryVel.context("mvel").velValue("memtest2", 1, 1, "2017-01-01 12:13:30");

    assertEquals(4, cnt);
 
    cnt = MemoryVel.context("mvel").velValue("memtest2", 1, 1, "2017-01-01 12:13:45");

    assertEquals(5, cnt);
 
    cnt = MemoryVel.context("mvel").velValue("memtest2", 1, 1, "2017-01-01 12:14:01");

    assertEquals(4, cnt);
 
    cnt = MemoryVel.context("mvel").velValue("memtest2", 1, 1, "2017-01-01 12:17:00");

    assertEquals(1, cnt);
 
    cnt = MemoryVel.context("mvel").velValue("memtest2", 2, 1, "2017-01-01 12:17:30");

    assertEquals(3, cnt);

    cnt = MemoryVel.context("mvel").velValue("memtest2", 2, 1, "2017-01-01 12:20:00");

    assertEquals(2, cnt);
  }

  /**
   * Simple value test
   */
  @Test
  public void testCountHits()
  {
    int origMax = MemoryVel.context("mvel").getMax();
    int max = origMax;

    for (int m = 0; m != 3; m++)
    {
      int cnt = 0;

      for (int i = 0; i != max; i++)
      {
        cnt = MemoryVel.context("mvel").velCount("memhittest" + m, 1, "2017-01-01 12:12:0" + i);

        assertEquals(i + 1, cnt);
      }

      cnt = MemoryVel.context("mvel").velCount("memhittest" + m, 1, "2017-01-01 12:12:" + String.format("%2.2s", max));

      assertNotEquals(max, cnt);

      switch(m)
      {
        case 0 : max = origMax * 2; break;
        case 1 : max = origMax / 2; break;
      }

      MemoryVel.context("mvel").setMax(max);
    }
  }
}
