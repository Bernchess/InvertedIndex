package ex2_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        // 把单词平均次数作为key,利用key的自动排序
        String[] str = tokens[1].split(",");

        // 输出相当于将单词和出现的平均次数换个位置
        outKey.set(str[0]);
        outValue.set(tokens[0]+","+str[1]);
        context.write(outKey, outValue);
    }
}