package edu.illinois.troups.client.tm;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.util.ReflectionUtils;

public abstract class RowGroupPolicy {

  private static final String META_ATTR = "ROW_GROUP_POLICY";

  /**
   * Implement to return group key for a given row
   * 
   * @param row
   * @return
   */
  public abstract byte[] getGroupKey(byte[] row);

  /**
   * utility for attaching row group policy to table descriptor
   * 
   * @param descr
   * @param cls
   */
  public static void setRowGroupPolicy(HTableDescriptor descr,
      Class<? extends RowGroupPolicy> cls) {
    descr.setValue(RowGroupPolicy.META_ATTR, cls.getName());
  }

  /**
   * utility for retrieving row group policy from table descriptor
   * 
   * @param descr
   * @return
   */
  @SuppressWarnings("unchecked")
  public static Class<RowGroupPolicy> getRowGroupPolicy(HTableDescriptor descr) {
    String className = descr.getValue(META_ATTR);
    if (className == null)
      return null;
    Class<?> cls;
    try {
      cls = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return (Class<RowGroupPolicy>) cls;
  }

  /**
   * creates a new instance of the row group policy if specified for the table.
   * Otherwise returns null.
   * 
   * @param table
   * @return
   * @throws IOException
   */
  public static RowGroupPolicy newInstance(HTable table) {
    HTableDescriptor descriptor;
    try {
      descriptor = table.getTableDescriptor();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Configuration conf = table.getConfiguration();
    return newInstance(descriptor, conf);
  }

  /**
   * creates a new instance of the row group policy if specified for the table.
   * Otherwise returns null.
   * 
   * @param descr
   * @param conf
   * @return
   */
  public static RowGroupPolicy newInstance(HTableDescriptor descr,
      Configuration conf) {
    RowGroupPolicy instance;
    Class<RowGroupPolicy> cls = getRowGroupPolicy(descr);
    if (cls == null)
      return null;
    Constructor<? extends RowGroupPolicy> cstr;
    try {
      try {
        cstr = cls.getConstructor(HTableDescriptor.class);
        instance = cstr.newInstance(descr);
      } catch (NoSuchMethodException e) {
        cstr = cls.getConstructor();
        instance = cstr.newInstance();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    ReflectionUtils.setConf(instance, conf);
    return instance;
  }

}