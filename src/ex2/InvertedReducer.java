package ex2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;

public class InvertedReducer extends Reducer<Text, LongWritable, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    private String lastWord = null;
    private StringBuilder outStr = new StringBuilder();
    /** 每个单词出现的文档数 */
    private long docCount = 0;
    /** 每个单词出现的总次数 */
    private long totalCount = 0;

    /**
     * 一个reduce方法只针对一个key值，不同key值的可能会被映射到同一台机器中
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // 把key相同的value加起来
        long sum = 0;
        for(LongWritable value : values) {
            sum += value.get();
        }

        String[] keys = key.toString().split("->");
        if(!keys[0].equals(lastWord)) {
            if (lastWord != null) {
                // 平均出现频次并四舍五入保留2位小数
                double avgCount = totalCount / (double)docCount;
                BigDecimal b = new BigDecimal(avgCount);
                avgCount = b.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                // 遇到不同的单词直接将上一个单词的词频统计信息写入输出文件
                outStr.replace(outStr.length()-1, outStr.length(),"");
                outKey.set(lastWord);
                outValue.set(avgCount + "," + outStr.toString());
                context.write(outKey, outValue);
            }
            // 将lastword设为第一次遇到的和上一个不同的单词
            lastWord = keys[0];
            outStr = new StringBuilder();
            outStr.append(keys[1]).append(":").append(sum).append(";");
            // 换个单词后文档数重新计数为1
            docCount = 1;
            totalCount = sum;
        } else {
            outStr.append(keys[1]).append(":").append(sum).append(";");
            // 每个reduce方法接收 <一个单词，所在文档名> 的key值，故一个单词执行了多少次reduce方法就表示有多个文档
            docCount++;
            totalCount += sum;
        }
    }

    /**
     * 在所有的reduce方法执行完毕后执行，只会执行一次
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(lastWord != null) {
            // 平均出现频次并四舍五入保留2位小数
            double avgCount = totalCount / (double)docCount;
            BigDecimal b = new BigDecimal(avgCount);
            avgCount = b.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            // 最后一个单词的信息未写入，在cleanup方法中进行写入
            outStr.replace(outStr.length()-1, outStr.length(),"");
            context.write(new Text(lastWord), new Text(avgCount + "," + outStr.toString()));
        }
        super.cleanup(context);
    }
}