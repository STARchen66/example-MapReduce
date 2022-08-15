package cn.hadoop.mapreduce.db.read;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DbReadMapper extends Mapper<LongWritable,GoodsBean,LongWritable, Text> {

    Text outValue = new Text();

    @Override
    protected void map(LongWritable key, GoodsBean value, Mapper<LongWritable, GoodsBean, LongWritable, Text>.Context context) throws IOException, InterruptedException {

        outValue.set(value.toString());

        context.write(key,outValue);
    }
}
