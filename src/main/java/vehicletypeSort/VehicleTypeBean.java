package vehicletypeSort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 1 实现writable接口,因为要排序，所以这里使用的是WritableComparable

public class VehicleTypeBean implements WritableComparable<VehicleTypeBean> {

    private long phoneNum;
    private long sumFlow;


    //2、有参无参构造
    public VehicleTypeBean() {
    }

    public VehicleTypeBean(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    /**
     * 对有参构造进行修改，提供一个set方法
     * 自己封装对象的set方法，用于对象属性赋值
     */
    public void set(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setPhoneNum(long phoneNum) {
        this.phoneNum = phoneNum;
    }
    //3、set和get方法

    public long getsumFlow() {
        return sumFlow;
    }

    public long getPhoneNum() {
        return phoneNum;
    }

    public void setsumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }


    //修改一下，返回的都是数据
//    @Override
//    public String toString() {
//        return sumFlow + "\t" + phoneNum;
//    }

    @Override
    public String toString() {
        return "" +sumFlow;
    }


    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(sumFlow);
        out.writeLong(phoneNum);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {

        this.sumFlow = in.readLong();
        this.phoneNum = in.readLong();
    }

    @Override
    public int compareTo(VehicleTypeBean o) {
        //当前的确诊数 - 参数的确诊数 > 0 ? -1
        return this.sumFlow - o.getsumFlow() > 0 ? -1 : (this.sumFlow - o.getsumFlow() < 0 ? 1 : 0);
    }

}
