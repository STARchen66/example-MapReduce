package cn.hadoop.mapreduce.covid.sumShort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CovidSortMapper extends Mapper<LongWritable, Text,CovidShortBean,Text> {

    CovidShortBean outKey = new CovidShortBean();
    Text outVale = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CovidShortBean, Text>.Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split("\t");

        outKey.set(Double.parseDouble(strings[1]),Double.parseDouble(strings[2]));
        outVale.set(strings[0]);

        context.write(outKey,outVale);
    }
}
