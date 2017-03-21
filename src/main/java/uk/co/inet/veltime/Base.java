package uk.co.inet.veltime;

import java.io.*;

abstract class Base implements Comparable, Serializable
{
  public abstract int compareTo(Object o);
}
