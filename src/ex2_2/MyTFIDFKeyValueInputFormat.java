package ex2_2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class MyTFIDFKeyValueInputFormat extends KeyValueTextInputFormat {
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}