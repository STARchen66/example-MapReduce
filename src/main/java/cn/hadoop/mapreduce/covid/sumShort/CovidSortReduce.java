package cn.hadoop.mapreduce.covid.sumShort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CovidSortReduce extends Reducer<CovidShortBean, Text,Text,CovidShortBean> {
    @Override
    protected void reduce(CovidShortBean key, Iterable<Text> values, Reducer<CovidShortBean, Text, Text, CovidShortBean>.Context context) throws IOException, InterruptedException {
        Text outKey = values.iterator().next();
        context.write(outKey,key);
    }
}
