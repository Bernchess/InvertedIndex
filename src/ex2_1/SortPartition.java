package ex2_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SortPartition extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        // 如何尽可能的平均划分？
        double num = Double.parseDouble(key.toString());
        int part = 0;
        if(num <= 5) {
            part = (int) num - 1;
        } else {
            part = (int) num / 5 + 4;
        }
        part = part < numPartitions ? part : numPartitions - 1;
        return numPartitions - part - 1;
    }
}