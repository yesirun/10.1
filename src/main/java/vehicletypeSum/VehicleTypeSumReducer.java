package vehicletypeSum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author:Ys
 * @date: 2022年08月10日 14:26
 * @desc:本类就是MapReduce程序中Reduce阶段的处理类对应都ReduceTask KEYIN:表示的是reduce 阶段输入kV水的类型对应着map的输出的key因此本需求中就是单词Text
 * VALUEIN:表示的是reduce阶段输Akv中v的类型对应者map的输出的value因此本需求中就是单词次数1 LongWritable
 * KEYOUT:表示的是reduce 阶段输出kv中k的类型跟业务相关 本需求中还是单词 Text
 * VALUEOUT:表示的是reduce阶段输出kv中v的类型跟业务相关 本需求中还是单词总次数 LongWritable
 */
public class VehicleTypeSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        //定义次数结果。
        LongWritable result = new LongWritable();

        //统计变量
        long count = 0;
        //遍历一组数据，取出该组所有的value  <VehicleType, Iterable[1,1,1]>
        for (LongWritable value : values) {
            // 所有的value累加 就是该车型的总次数
            count += value.get(); // 0 + 1 + 1 + 1 = 3
        }
        result.set(count);
        //输出最终结果<车型，总次数>
        //所有的输出，按照key的字典升序进行排序（默认）
        context.write(key, result);

    }
}
