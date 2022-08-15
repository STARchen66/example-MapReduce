package cn.hadoop.mapreduce.covid.sum;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CovidSumMapper extends Mapper<LongWritable, Text,Text,CovidCountBean> {

    Text outKey = new Text();
    CovidCountBean outValue = new CovidCountBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CovidCountBean>.Context context) throws IOException, InterruptedException {
        //读取一行数据进行切割
        String[] csvComments = value.toString().split(",");
        //提取数据
        if(!csvComments[3].equals("Country/Region")){
            outKey.set(csvComments[csvComments.length-5]);
        }

        if(!(csvComments[5].equals("Confirmed"))&&!(csvComments[6].equals("Deaths"))){
            outValue.set(Double.parseDouble(csvComments[csvComments.length-3]),Double.parseDouble(csvComments[csvComments.length-2]));
        }

        //输出结果 <州，CovidCountBean>
        context.write(outKey,outValue);
    }
}
