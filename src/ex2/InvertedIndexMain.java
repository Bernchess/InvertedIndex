package ex2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class InvertedIndexMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 为任务设定配置文件
        Configuration conf = new Configuration();

        // 命令行参数
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: <inputDir> <outputDir>");
            System.exit(2);
        }

        // 新建一个用户定义的Job并配置
        Job job = Job.getInstance(conf, "InvertIndex");
        job.setJarByClass(InvertedIndexMain.class);
        job.setMapperClass(InvertedMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setPartitionerClass(InvertedPartitioner.class);
        job.setReducerClass(InvertedReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入文件的路径
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // 设置输出文件的路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // 提交任务并等待任务完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}