package vehicletypeSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//<数量,车型>  => <FlowBean,Text>
public class VehicleTypeSortMapper extends Mapper<LongWritable, Text, VehicleTypeBean, Text> {

    VehicleTypeBean k = new VehicleTypeBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();

        // 2 切割字段
        String[] fields = line.split("\t");
        //String str1 = "Saloon\t851";

        // 3 封装对象
        // 取出车型
        String VehicleType = fields[0].trim();
        // 取出数量
        long count = Long.parseLong(fields[1].trim());

        v.set(VehicleType);
        k.set(count);

        // 4 写出
        context.write(k, v);//（数量，车型）

    }
}
