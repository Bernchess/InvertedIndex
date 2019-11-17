package ex2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class InvertedPartitioner extends Partitioner<Text, Object> {
    @Override
    public int getPartition(Text key, Object value, int numReduceTasks) {
        String term = key.toString().split("->")[0];
        return (term.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}