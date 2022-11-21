package vehicletypeSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author:Ycr
 * @date:
 * @desc:
 */



public class VehicleTypeSortDriver {

    public static void main(String[] args) throws Exception {

        //创建驱动类
        Configuration conf = new Configuration();

        //构造job作业的实例,参数（配置对象，job名字）
        Job job = Job.getInstance(conf, VehicleTypeSortDriver.class.getSimpleName());

        //设置mr程序运行的主类
        job.setJarByClass(VehicleTypeSortDriver.class);

        //设置本次mr程序的mapper类型、reducer类型
        job.setMapperClass(VehicleTypeSortMapper.class);
        job.setReducerClass(VehicleTypeSortReducer.class);

        job.setMapOutputKeyClass(VehicleTypeBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VehicleTypeBean.class);

        //配置本次作业的输入数据路径和输出数据路径
        Path inputPath = new Path("output_vehicletypeSum");
        Path outputPath = new Path("output_vehicletypeSort");

        //todo 默认组件 TextInputFormat TextOutputFormat
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        //todo 判断输出路径是否已经存在，如果已经存在，先删除
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true); //递归删除
        }

        //最终提交本次job作业
        //job.submit();
        //采用waitForCompletion提交job，参数表示是否开启实时监视追踪作业的执行情况
        boolean resultFlag = job.waitForCompletion(true);
        //退出程序 和job结果进行绑定, 0是正常退出，1是异常退出
        System.exit(resultFlag ? 0: 1);

    }
}

