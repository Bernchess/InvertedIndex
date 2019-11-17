package ex2_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class InvertedIndexSortMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 为任务设定配置文件
        Configuration conf = new Configuration();

        // 命令行参数
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3) {
            System.err.println("Usage: <inputDir> <outputDir> <sortOutputDir>");
            System.exit(-1);
        }

        /**---------新建用于统计词频的job并进行配置-------------------------------------------------*/
        Job job = Job.getInstance(conf, "InvertIndex");
        job.setJarByClass(InvertedIndexSortMain.class);
        job.setMapperClass(InvertedMapper_2.class);
        job.setPartitionerClass(ex2.InvertedPartitioner.class);
        job.setCombinerClass(InvertedCombiner_2.class);
        job.setReducerClass(InvertedReducer_2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入文件的路径
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // 设置输出文件的路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // 设置文件不允许被分片，否则在计算单词出现的文档数时会出错（分片的个数会被当作文档数）
//        job.setInputFormatClass(ex2_2.MyTFIDFTextInputFormat.class);
        /**----------end------------------------------------------------------------------------*/

        /**---------新建用于排序上一个任务的输出的job并进行配置--------------------------------------*/
        // 新建用于排序的job
        Job sortJob = Job.getInstance(conf, "InvertedIndexSort");
        sortJob.setJarByClass(InvertedIndexSortMain.class);
        sortJob.setMapperClass(SortMapper.class);
        sortJob.setSortComparatorClass(SortCompare.class);
        sortJob.setPartitionerClass(SortPartition.class);
        sortJob.setReducerClass(SortReducer.class);

        sortJob.setMapOutputKeyClass(Text.class);
        sortJob.setMapOutputValueClass(Text.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sortJob, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[2]));
        /**----------end-----------------------------------------------------------------------*/

        // 提交任务并等待任务完成
        if(job.waitForCompletion(true)) {
            System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
        }
    }
}