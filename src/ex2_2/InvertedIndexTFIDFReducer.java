package ex2_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InvertedIndexTFIDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();
    private int docTotalCount = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        docTotalCount = Integer.parseInt(context.getConfiguration().get("numInputFiles"));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Text> valuesClone = new ArrayList<>();
        int docCount = 1; // 包含该单词的文档数
        // 第一次遍历计算文档数
        for(Text value : values) {
            valuesClone.add(new Text(value));
            String[] tokens = value.toString().split("->");
            docCount += Integer.parseInt(tokens[1]);
        }
        // 第二次遍历计算TF-IDF并写到输出文件
        for(Text value : valuesClone) {
            String[] tokens = value.toString().split("->");
            outKey.set(tokens[2]);
            outValue.set(key.toString() + ", TF = " + tokens[0] + ", IDF = lg(" +docTotalCount+"/" + docCount + ") = " + Math.log10(docTotalCount / (double)docCount) + ", TF-IDF: " + Double.parseDouble(tokens[0])*Math.log10(docTotalCount / (double)docCount));
            context.write(outKey, outValue);
        }
        valuesClone.clear();
    }
}

//public class InvertedTFIDFReducer extends Reducer<Text, Text, Text, Text> {
//    private Text outKey = new Text();
//    private Text outValue = new Text();
//    // 包含该单词的文档数
//    private int docCount = 1;
//    private int docTotalCount = 0;
//    private String lastWord = null;
//    private String lastAuthor = null;
//
//    private Vector<String> Authors = new Vector<>();
//    private Vector<Long> TFs = new Vector<>();
//
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        docTotalCount = Integer.parseInt(context.getConfiguration().get("numInputFiles"));
//    }
//
//    @Override
//    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        String[] tokens = key.toString().split("->");
//        long tf = 0;
//        if(!tokens[0].equals(lastWord)) {
//            if(lastWord != null) {
//                for(int i=0; i<Authors.size(); ++i) {
//                    outValue.set(Authors.get(i) + ", " + lastWord + ", " + TFs.get(i) + ", " +
//                            "lg(" + docTotalCount + " / " + docCount + ") = "+Math.log10(docTotalCount/(double)docCount));
//                    context.write(outKey, outValue);
//                }
//                Authors.clear();
//                TFs.clear();
//            }
//            docCount = 1;
//            lastWord = tokens[0];
//            lastAuthor = tokens[1];
//            for(Text t : values) {
//                docCount++;
//                tf += Long.parseLong(t.toString());
//            }
//            Authors.add(lastAuthor);
//            TFs.add(tf);
//            return ;
//        }
//        if(!tokens[1].equals(lastAuthor)) {
//            lastAuthor = tokens[1];
//            for(Text t : values) {
//                docCount++;
//                tf += Long.parseLong(t.toString());
//            }
//            Authors.add(lastAuthor);
//            TFs.add(tf);
//        }
//    }
//
//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        // 还有最后一个单词没有写入
//        for(int i=0; i<Authors.size(); ++i) {
//            outValue.set(Authors.get(i) + ", " + lastWord + ", " + TFs.get(i) + ", " +
//                    "lg(" + docTotalCount + " / " + docCount + ") = "+Math.log10(docTotalCount/(double)docCount));
//            context.write(outKey, outValue);
//        }
//        Authors.clear();
//        TFs.clear();
//    }
//}
