package ex2_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;

public class InvertedReducer_2 extends Reducer<Text, Text, Text, Text> {
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;
        long wordCount = 0;
        StringBuilder outStr = new StringBuilder();
        for(Text t : values) {
            docCount++;
            String[] tokens = t.toString().split("->");
            wordCount += Long.parseLong(tokens[1]);
            outStr.append(tokens[0]).append(":").append(tokens[1]).append(";");
        }
        outStr.replace(outStr.length()-1, outStr.length(), "");

        double avgCount = wordCount / (double)docCount;
        BigDecimal b = new BigDecimal(avgCount);
        avgCount = b.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();

        outStr.insert(0,avgCount + ",");
        outValue.set(outStr.toString());
        context.write(key, outValue);
    }
}
