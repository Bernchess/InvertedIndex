package ex2_2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 * 此Mapper和LongSumReducer的job计算一个作者的文章的词的总数
 */
public class InvertedIndexTFMapper extends Mapper<Object, Text, Text, LongWritable> {
    private final static LongWritable ONE = new LongWritable(1);
    private Text outKey = new Text();
    private String author = "";
    private final static Pattern r = Pattern.compile("[\u4e00-\u9fa5a-zA-Z]");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 通过文件名获取作者姓名
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileNameTmp = fileSplit.getPath().getName();
        char[] chars = fileNameTmp.toCharArray();
        for(char c : chars) {
            // 判断字符是否为汉字或字母
            if(!r.matcher(String.valueOf(c)).matches()) {
                break;
            }
            author += c;
        }
        if(author.length() == 0) {
            author = "UnknownAuthor";
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens()) {
            outKey.set(author + "->" + itr.nextToken());
            context.write(outKey, ONE);
        }
    }
}