package ex2_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class InvertedIndexTFIDFMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 为任务设定配置文件
        Configuration conf = new Configuration();

        // 命令行参数
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3) {
            System.err.println("Usage: <inputDir> <outputDir1> <outputDir2>");
            System.exit(-1);
        }

        // 获取文件数量并保存到配置中，reducer中需要使用
        Path path = new Path(args[0]);
        FileSystem hdfs = path.getFileSystem(conf);
        conf.set("numInputFiles", hdfs.listStatus(path).length+"");

        /**---------新建用于计算“某单词在某作者文章中出现的次数”的job并进行配置------------------------------*/
        Job countJob = Job.getInstance(conf, "InvertedIndexTFIDF_1");
        countJob.setJarByClass(InvertedIndexTFIDFMain.class);
        countJob.setMapperClass(InvertedIndexTFMapper.class);
        countJob.setCombinerClass(LongSumReducer.class);
        countJob.setPartitionerClass(ex2.InvertedPartitioner.class);
        countJob.setReducerClass(InvertedIndexTFReducer.class);

        countJob.setMapOutputKeyClass(Text.class);
        countJob.setMapOutputValueClass(LongWritable.class);
        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(Text.class);

        // 设置输入文件的路径
        FileInputFormat.addInputPath(countJob, new Path(otherArgs[0]));
        // 设置输出文件的路径
        FileOutputFormat.setOutputPath(countJob, new Path(otherArgs[1]));

        // 在reduce方法用value的个数来计算文档数有问题，若输入文档过大系统会自动进行分片成多份文档，在这里设置输入文件不允许分片
        countJob.setInputFormatClass(MyTFIDFTextInputFormat.class);
        /**----------end----------------------------------------------------------------------------*/

        /**---------新建用于计算TF-IDF的job并进行配置--------------------------------------------------*/
        Job job = Job.getInstance(conf, "InvertedIndexTFIDF_2");
        job.setJarByClass(InvertedIndexTFIDFMain.class);
        job.setMapperClass(InvertedIndexTFIDFMapper.class);
        job.setReducerClass(InvertedIndexTFIDFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入文件的路径
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        // 设置输出文件的路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        // 使用上一个job的输出结果，直接解析为KV对,并且不允许对文件进行分片，因为一个文件保存一个作者的文章信息
        job.setInputFormatClass(MyTFIDFKeyValueInputFormat.class);
        /**----------end----------------------------------------------------------------------------*/

        // 提交任务并等待任务完成
        if(countJob.waitForCompletion(true)) {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}