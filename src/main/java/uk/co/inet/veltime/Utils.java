package uk.co.inet.veltime;

import java.io.*;
import java.util.*;

public class Utils
{
  public final static Base toBase(byte[] bs)
  {
    Base base = null;

    if (bs.length == 4)
    {
      int ts = byteToInt(bs, 0);
    
      base = new Item(ts, 1);
    }
    else if (bs.length == 8)
    {
      int ts = byteToInt(bs, 0);
      int item = byteToInt(bs, 4);
    
      base = new Item(ts, item);
    }
    else if (bs.length > 8)
    {
      Item[] items = new Item[bs.length / 8];

      for (int i = 0; i != bs.length; i += 8)
      {
        int ts = byteToInt(bs, i + 0);
        int item = byteToInt(bs, i + 4);

        items[i / 8] = new Item(ts, item);
      }
    
      base = new Items(items);
    }

    return base;
  }

  public final static byte[] fromBase(Base item)
  {
    byte[] bs = null;

    if (item instanceof Item)
    {
      bs = new byte[8];

      intToByte(bs, ((Item) item).ts, 0);
      intToByte(bs, ((Item) item).item, 4);
    }
    else if (item instanceof Items)
    {
//System.err.println("Items : " + item);
      Item[] items = ((Items) item).items;
      int len = 8 * items.length;

      bs = new byte[len];

      for (int i = 0; i != len; i += 8)
      {
        int j = i / 8;

        intToByte(bs, items[j].ts, i + 0);
        intToByte(bs, items[j].item, i + 4);
      }
    }

    return bs;
  }

  public static int intToByte(byte[] arrayDst, int org, int offset)
  {
    if (arrayDst == null || arrayDst.length < offset + 4)
      return 0;

    int idxDst = offset;

    arrayDst[idxDst++] = (byte)(org);
    arrayDst[idxDst++] = (byte)(org >>> 8);
    arrayDst[idxDst++] = (byte)(org >>> 16);
    arrayDst[idxDst++] = (byte)(org >>> 24);

    return idxDst;
  }

  public static int longToByte(byte[] arrayDst, long org, int offset)
  {
    if (arrayDst == null || arrayDst.length < offset + 8)
      return 0;

    int idxDst = offset;

    arrayDst[idxDst++] = (byte)(org);
    arrayDst[idxDst++] = (byte)(org >>> 8);
    arrayDst[idxDst++] = (byte)(org >>> 16);
    arrayDst[idxDst++] = (byte)(org >>> 24);
    arrayDst[idxDst++] = (byte)(org >>> 32);
    arrayDst[idxDst++] = (byte)(org >>> 40);
    arrayDst[idxDst++] = (byte)(org >>> 48);
    arrayDst[idxDst++] = (byte)(org >>> 56);

    return idxDst;
  }

  /*
  private static int intToByte(byte[] arrayDst, int[] arrayOrg, int offset)
  {
    int maxOrg = arrayOrg.length;
    int maxDst = maxOrg * 4;

    if (arrayDst == null || arrayOrg == null)
      return 0;
    if (arrayDst.length < maxDst || arrayOrg.length < maxOrg)
      return 0;

    int idxDst = offset;

    for (int i = 0; i < maxOrg; i++)
    {
      arrayDst[idxDst++] = (byte)(arrayOrg[i]);
      arrayDst[idxDst++] = (byte)(arrayOrg[i] >> 8);
      arrayDst[idxDst++] = (byte)(arrayOrg[i] >> 16);
      arrayDst[idxDst++] = (byte)(arrayOrg[i] >> 24);
    }

    return idxDst;
  }
  */

  public static int byteToInt(byte[] arrayOrg, int offset)
  {
    if (arrayOrg == null || arrayOrg.length < offset + 4)
      return 0;

    int idxOrg = offset;
    int v;
    int dst = 0;

    v = 0x000000FF & arrayOrg[idxOrg++];
    dst = dst | v;
    v = 0x000000FF & arrayOrg[idxOrg++];
    dst = dst| (v << 8);
    v = 0x000000FF & arrayOrg[idxOrg++];
    dst = dst | (v << 16);
    v = 0x000000FF & arrayOrg[idxOrg++];
    dst = dst | (v << 24);

    return dst;
  }

  public static long byteToLong(byte[] arrayOrg, int offset)
  {
    if (arrayOrg == null || arrayOrg.length < offset + 8)
      return 0;

    int idxOrg = offset;
    int v;
    int dst = 0;

    v = 0x000000FF & arrayOrg[idxOrg++];
    dst = dst | v;
    v = 0x000000FF & arrayOrg[idxOrg++];
    dst = dst| (v << 8);
    v = 0x000000FF & arrayOrg[idxOrg++];
    dst = dst | (v << 16);
    v = 0x000000FF & arrayOrg[idxOrg++];
    dst = dst | (v << 24);
    v = 0x000000FF & arrayOrg[idxOrg++];
    dst = dst | (v << 32);
    v = 0x000000FF & arrayOrg[idxOrg++];
    dst = dst | (v << 40);
    v = 0x000000FF & arrayOrg[idxOrg++];
    dst = dst | (v << 48);
    v = 0x000000FF & arrayOrg[idxOrg++];
    dst = dst | (v << 56);

    return dst;
  }

  /*
  private static int byteToInt(int[] arrayDst, byte[] arrayOrg)
  {
    int maxOrg = arrayOrg.length;
    int maxDst = maxOrg / 4;

    if (arrayDst == null || arrayOrg == null)
      return 0;
    if (arrayDst.length < maxDst || arrayOrg.length < maxOrg)
      return 0;

    int idxOrg = 0;

    for (int i = 0; i < maxDst; i++)
    {
      int v;

      arrayDst[i] = 0;

      v = 0x000000FF & arrayOrg[idxOrg++];
      arrayDst[i] = arrayDst[i] | v;
      v = 0x000000FF & arrayOrg[idxOrg++];
      arrayDst[i] = arrayDst[i] | (v << 8);
      v = 0x000000FF & arrayOrg[idxOrg++];
      arrayDst[i] = arrayDst[i] | (v << 16);
      v = 0x000000FF & arrayOrg[idxOrg++];
      arrayDst[i] = arrayDst[i] | (v << 24);
    }

    return maxDst;
  }
  */
}
