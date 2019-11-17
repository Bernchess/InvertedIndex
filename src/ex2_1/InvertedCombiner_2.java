package ex2_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedCombiner_2 extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for(Text value : values) {
            // 当单个文件过大（测试100MB）会出现value不是数字的异常，为什么？使用ex2包中的代码计算不会出现问题
            sum += Long.parseLong(value.toString());
        }
        String[] keys = key.toString().split("->");
        outKey.set(keys[0]);
        outValue.set(keys[1] + "->" + sum);
        context.write(outKey, outValue);
    }
}
