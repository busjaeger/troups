package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;

import edu.illinois.troups.client.tm.RowGroupPolicy;

public class RowGroupSplitPolicy extends ConstantSizeRegionSplitPolicy {

  private RowGroupPolicy policy;

  @Override
  void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    this.policy = newInstance(region);
    if (policy == null)
      throw new IllegalStateException("No row group strategy configured");
  }

  @Override
  byte[] getSplitPoint() {
    return policy.getGroupKey(super.getSplitPoint());
  }

  // server-side API
  public static RowGroupPolicy newInstance(HRegion region) {
    HTableDescriptor descriptor = region.getTableDesc();
    Configuration conf = region.getConf();
    return RowGroupPolicy.newInstance(descriptor, conf);
  }
}
