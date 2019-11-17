package ex2_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 在reduce中再将单词和平均次数位置换过来即可得到最终结果
        for(Text t : values) {
            String[] tokens = t.toString().split(",");
            outKey.set(tokens[0]);
            outValue.set(key.toString() + "," + tokens[1]);
            context.write(outKey, outValue);
        }
    }
}