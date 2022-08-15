package cn.hadoop.mapreduce.covid.sumShort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CovidShortBean implements WritableComparable<CovidShortBean> {
    private double cases;
    private double deaths;

    public CovidShortBean() {
    }

    public CovidShortBean(double cases, double deaths) {
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

    @Override
    public int compareTo(CovidShortBean o) {
        return this.cases - o.getCases() > 0 ? -1 : (this.cases - o.getCases() < 0 ? 1 : 0);
    }

}
