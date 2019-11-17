package ex2_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Vector;

public class InvertedIndexTFIDFMapper extends Mapper<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    private long wordTotalCount = 0;
    private Vector<String> words = new Vector<>();
    private Vector<Integer> wordCount = new Vector<>();
    private Vector<Integer> numFiles = new Vector<>();
    private Vector<String> author = new Vector<>();

    private String lastAuthor = null;

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // key: 作者->单词  value: 单词个数->文档个数
        String[] keys = key.toString().split("->");
        String[] tokens = value.toString().split("->");
        if(!keys[0].equals(lastAuthor)) {
            // 写入上一个作者的信息
            if(lastAuthor != null) {
                double TF = 0;
                for (int i = 0; i < words.size(); ++i) {
                    outKey.set(words.get(i));
                    TF = wordCount.get(i) / (double) wordTotalCount;
                    outValue.set(TF + "->" + numFiles.get(i) + "->" + author.get(i));
                    context.write(outKey, outValue);
                }
            }
            // 开始下一个作者，变量重置
            lastAuthor = keys[0];
            wordTotalCount = 0;
            words.clear();
            wordCount.clear();
            numFiles.clear();
            author.clear();
        }
        // 保存文件中的内容
        author.add(keys[0]);
        words.add(keys[1]);
        wordCount.add(Integer.parseInt(tokens[0]));
        numFiles.add(Integer.parseInt(tokens[1]));
        // 统计该作者文章的总词数
        wordTotalCount += Long.parseLong(tokens[0]);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        double TF = 0;
        for(int i=0; i<words.size(); ++i) {
            outKey.set(words.get(i));
            TF = wordCount.get(i) / (double)wordTotalCount;
            outValue.set(TF + "->" + numFiles.get(i) + "->" + author.get(i));
            context.write(outKey, outValue);
        }
        words.clear();
        wordCount.clear();
        numFiles.clear();
        author.clear();
    }
}
