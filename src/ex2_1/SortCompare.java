package ex2_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortCompare extends WritableComparator {
    protected SortCompare() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        double aa = Double.parseDouble(a.toString());
        double bb = Double.parseDouble(b.toString());
        // 实现从大到小排序
        return Double.compare(bb, aa);
    }
}