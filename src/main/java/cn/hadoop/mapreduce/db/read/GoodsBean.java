package cn.hadoop.mapreduce.db.read;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GoodsBean implements Writable, DBWritable {
    private String country;//国家
    private long activeDuty;//活跃军人数
    private long paramilitary;//准军事人员
    private long reserves;//军事储备
    private long total;
    private double pop2022;

    public GoodsBean() {
    }

    public GoodsBean(String country, long activeDuty, long paramilitary, long reserves, long total, double pop2022) {
        this.country = country;
        this.activeDuty = activeDuty;
        this.paramilitary = paramilitary;
        this.reserves = reserves;
        this.total = total;
        this.pop2022 = pop2022;
    }

    public void set(String country, long activeDuty, long paramilitary, long reserves, long total, double pop2022) {
        this.country = country;
        this.activeDuty = activeDuty;
        this.paramilitary = paramilitary;
        this.reserves = reserves;
        this.total = total;
        this.pop2022 = pop2022;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public long getActiveDuty() {
        return activeDuty;
    }

    public void setActiveDuty(long activeDuty) {
        this.activeDuty = activeDuty;
    }

    public long getParamilitary() {
        return paramilitary;
    }

    public void setParamilitary(long paramilitary) {
        this.paramilitary = paramilitary;
    }

    public long getReserves() {
        return reserves;
    }

    public void setReserves(long reserves) {
        this.reserves = reserves;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public double getPop2022() {
        return pop2022;
    }

    public void setPop2022(double pop2022) {
        this.pop2022 = pop2022;
    }

    @Override
    public String toString() {
        return "GoodsBean{" +
                "country='" + country + '\'' +
                ", activeDuty=" + activeDuty +
                ", paramilitary=" + paramilitary +
                ", reserves=" + reserves +
                ", total=" + total +
                ", pop2022=" + pop2022 +
                '}';
    }

    /*
    private String country;//国家
    private long activeDuty;//活跃军人数
    private long paramilitary;//准军事人员
    private long reserves;//军事储备
    private long total;
    private double pop2022;
     */

    //序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(country);
        dataOutput.writeLong(activeDuty);
        dataOutput.writeLong(paramilitary);
        dataOutput.writeLong(reserves);
        dataOutput.writeLong(total);
        dataOutput.writeDouble(pop2022);
    }

    //反序列化方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.country=dataInput.readUTF();
        this.activeDuty=dataInput.readLong();
        this.paramilitary=dataInput.readLong();
        this.reserves=dataInput.readLong();
        this.total=dataInput.readLong();
        this.pop2022=dataInput.readDouble();
    }

    //设置对象的字段，写数据操作
    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1,country);
        statement.setLong(2,activeDuty);
        statement.setLong(3,paramilitary);
        statement.setLong(4,reserves);
        statement.setLong(5,total);
        statement.setDouble(6,pop2022);
    }

    //读取查询结果，赋值给对象属性   读数据库操作
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.country=resultSet.getString(1);
        this.activeDuty=resultSet.getLong(2);
        this.paramilitary=resultSet.getLong(3);
        this.reserves=resultSet.getLong(4);
        this.total=resultSet.getLong(5);
        this.pop2022=resultSet.getDouble(6);
    }
}
