package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.util.ReflectionUtils;

import edu.illinois.htx.client.tm.RowGroupPolicy;

public class RowGroupSplitPolicy extends ConstantSizeRegionSplitPolicy {

  private RowGroupPolicy strategy;

  @Override
  void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    this.strategy = getRowGroupStrategy(region);
    if (strategy == null)
      throw new IllegalStateException("No row group strategy configured");
  }

  @Override
  byte[] getSplitPoint() {
    return strategy.getGroupRow(super.getSplitPoint());
  }

  // server-side API
  public static RowGroupPolicy getRowGroupStrategy(HRegion region) {
    HTableDescriptor descriptor = region.getTableDesc();
    Configuration conf = region.getConf();
    return getRowGroupStrategy(descriptor, conf);
  }

  // client-side API
  public static RowGroupPolicy getRowGroupStrategy(HTable table)
      throws IOException {
    HTableDescriptor descriptor = table.getTableDescriptor();
    Configuration conf = table.getConfiguration();
    return getRowGroupStrategy(descriptor, conf);
  }

  private static RowGroupPolicy getRowGroupStrategy(
      HTableDescriptor descriptor, Configuration conf) {
    String className = descriptor.getValue(RowGroupPolicy.META_ATTR);
    if (className == null)
      return null;

    RowGroupPolicy strategy;
    try {
      Class<?> cls = Class.forName(className);
      @SuppressWarnings("unchecked")
      Class<? extends RowGroupPolicy> rspCls = (Class<? extends RowGroupPolicy>) cls;
      Constructor<? extends RowGroupPolicy> cstr;
      try {
        cstr = rspCls.getConstructor(HTableDescriptor.class);
        strategy = cstr.newInstance(descriptor);
      } catch (NoSuchMethodException e) {
        cstr = rspCls.getConstructor();
        strategy = cstr.newInstance();
      }
    } catch (Exception e) {
      throw new IllegalStateException("Invalid row group strategy configured",
          e);
    }
    ReflectionUtils.setConf(strategy, conf);
    return strategy;
  }
}
