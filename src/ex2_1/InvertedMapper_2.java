package ex2_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedMapper_2 extends Mapper<Object, Text, Text, Text> {
    private final static Text ONE = new Text("1");
    private Text outKey = new Text();
    private String fileName = "";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取文件名
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
        int endIndex = fileName.indexOf(".txt");
        if(endIndex < 0) {
            endIndex = fileName.indexOf(".TXT");
        }
        fileName = endIndex > 0 ? fileName.substring(0, endIndex) : fileName;
    }

    /**
     * @param key 每一行的行偏移值
     * @param value 对应一行的值
     * @param context 用于写最终的key和value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens()) {
            outKey.set(itr.nextToken() + "->" + fileName);
            context.write(outKey, ONE);
        }
    }
}