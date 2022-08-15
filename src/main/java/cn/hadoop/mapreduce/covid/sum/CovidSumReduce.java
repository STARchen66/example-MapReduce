package cn.hadoop.mapreduce.covid.sum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CovidSumReduce extends Reducer<Text,CovidCountBean,Text,CovidCountBean> {

    CovidCountBean outValue = new CovidCountBean();

    @Override
    protected void reduce(Text key, Iterable<CovidCountBean> values, Reducer<Text, CovidCountBean, Text, CovidCountBean>.Context context) throws IOException, InterruptedException {
        //统计变量
        long totalCases = 0;
        long totalDeaths = 0;
        //遍历该州各个县数据
        for (CovidCountBean value : values) {
            totalCases += value.getCases();
            totalDeaths += value.getDeaths();
        }
        //赋值
        outValue.set(totalCases,totalDeaths);
        //输出结果
        context.write(key,outValue);
    }
}
