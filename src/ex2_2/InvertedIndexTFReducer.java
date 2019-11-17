package ex2_2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexTFReducer extends Reducer<Text, LongWritable, Text, Text> {
    Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        int docCount = 0;
        for(LongWritable value : values) {
            sum += value.get();
            docCount++;
        }
        outValue.set(sum + "->" + docCount);
        context.write(key, outValue);
    }
}