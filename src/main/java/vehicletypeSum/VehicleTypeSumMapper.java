package vehicletypeSum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author:Ys
 * @date: 2022年08月10日 14:20
 * @desc:WordCount Mapper类对应者Map task
 * <p>
 * KEYIN:表示map阶段输入kV中的k类型在默认组件下 是起始位置偏移量因此是LongWritable  文字所在的行数
 * VALUEIN:表示map阶段输入kV中的v类型在默认組件下 是每一行内容因此是Text. 所以一行文本
 * todo MapReduce 有默认的读取数据组件叫做TextInputFormat
 * todo读数据的行为是: - 行一行读取数据返回kv 键值对   key:1 value:hadoop hadoop
 * k:每-行的起始位置的偏移量通常无意义
 * v:这一行的文本内容
 * KEYOUT: 表示map阶段输出kV中的k类型跟业务相关 本需求中输出的是单词因此是Text  key: hadoop  value:1
 * VALUEOUT:表示map 阶段输出kV中的V类型跟业务相关 本需求中输出的是单词次数1因此是LongWritable
 */
public class VehicleTypeSumMapper extends Mapper<LongWritable, Text, Text, LongWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Mapper输出kv键值对  <车型，1>
        Text outKey = new Text();
        LongWritable valueOut = new LongWritable(1);

        //将读取的一行内容根据分隔符进行切割
        String[] fields = value.toString().split(",");  // [VehicleType VehicleType VehicleType] == [VehicleType,VehicleType,VehicleType]

        //获得车型
        String VehicleType = fields[4];
        outKey.set(VehicleType);

        //输出车型，并标记为1
        context.write(outKey, valueOut);

    }


}

