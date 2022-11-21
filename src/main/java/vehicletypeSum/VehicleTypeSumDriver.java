package vehicletypeSum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author:Ys
 * @date: 2022年08月10日 14:36
 * @desc:该类就是MapReduce程序客户端驱动类主要是构造Job对象实例 指定各种组件属性包括: mapper reducer类 输入输出的数据类型、输入输出的数据路径
 * 提交job作业job.submit()
 * <p>
 * 利用单词统计的代码 统计某个英文小说的某个自然段中的单词出现的次数
 */
public class VehicleTypeSumDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //进行log4j的初始化
        //initLogRecord.initLog();

        //配置文件对象
        Configuration conf = new Configuration();//本地文件
        //conf.set("fs.defaultFS","hdfs://node3:9870");//hdfs文件系统

        // 创建作业实例
        Job job = Job.getInstance(conf, VehicleTypeSumDriver.class.getSimpleName());
        // 设置作业驱动类
        job.setJarByClass(VehicleTypeSumDriver.class);

        // 设置作业mapper reducer类
        job.setMapperClass(VehicleTypeSumMapper.class);
        //在Driver类中去设置combiner 但是combiner不能乱用。因为在处理求平均值的时候会有错误的产生
        job.setCombinerClass(VehicleTypeSumReducer.class);
        //mapReducer的程序 一定会有mapper,但是不一定有reducer
        job.setReducerClass(VehicleTypeSumReducer.class);


        // 设置作业mapper阶段输出key value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置作业reducer阶段输出key value数据类型 也就是程序最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 配置作业的输入数据路径
        FileInputFormat.addInputPath(job, new Path("input"));
        // 配置作业的输出数据路径
        Path outputDir = new Path("output_vehicletypeSum");
        FileOutputFormat.setOutputPath(job, outputDir);
        //判断输出路径是否存在 如果存在删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        // 提交作业并等待执行完成
        boolean resultFlag = job.waitForCompletion(true);
        //程序退出
        System.exit(resultFlag ? 0 : 1);
    }
}
