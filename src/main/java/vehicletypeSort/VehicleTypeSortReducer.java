package vehicletypeSort;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//<数量,车型>  ===> <车型,数量>
public class VehicleTypeSortReducer extends Reducer<VehicleTypeBean,Text , Text, VehicleTypeBean> {
    @Override
    protected void reduce(VehicleTypeBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text text : values) {
            context.write(text, key);
        }
    }
}
