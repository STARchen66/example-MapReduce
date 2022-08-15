package cn.hadoop.mapreduce.covid.sum;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
* todo 自定义对象在mapreduce中作为数据类型传递，一定要实现hadoop序列化
* */
public class CovidCountBean implements Writable {

    private double cases;
    private double deaths;

    public CovidCountBean() {
    }

    public CovidCountBean(double cases, double deaths) {
        this.cases = cases;
        this.deaths = deaths;
    }
    public void set(double cases, double deaths) {
        this.cases = cases;
        this.deaths = deaths;
    }

    public double getCases() {
        return cases;
    }

    public void setCases(double cases) {
        this.cases = cases;
    }

    public double getDeaths() {
        return deaths;
    }

    public void setDeaths(double deaths) {
        this.deaths = deaths;
    }

    @Override
    public String toString() {
        return  cases+"   "+deaths;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(cases);
        dataOutput.writeDouble(deaths);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.cases = dataInput.readDouble();
        this.deaths = dataInput.readDouble();
    }
}
