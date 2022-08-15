package cn.hadoop.mapreduce.covid.sumShort;

import cn.hadoop.mapreduce.covid.sum.CovidCountBean;
import cn.hadoop.mapreduce.covid.sum.CovidSumMapper;
import cn.hadoop.mapreduce.covid.sum.CovidSumReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CovidSortDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        //创建配置对象
        Configuration conf = new Configuration();
        //判断输出路径是否存在，存在则删除
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]), true);
        }

        //使用工具类提交程序
        int status = ToolRunner.run(conf,new cn.hadoop.mapreduce.covid.sumShort.CovidSortDriver(),args);
        //退出客户端
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        //构建job工作的实例，参数（配置对象，job名）
        Job job = Job.getInstance();
        //设置mr程序主类
        job.setJarByClass(CovidSortDriver.class);

        //设置本次mr程序的mapper类型 reduce类
        job.setMapperClass(CovidSortMapper.class);
        job.setReducerClass(CovidSortReduce.class);

        //指定mapper阶段输出的key value数据类型
        job.setMapOutputKeyClass(CovidShortBean.class);
        job.setMapOutputValueClass(Text.class);

        //指定reduce阶段输出的key value类型 也是mr程序最终的输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CovidShortBean.class);

        //配置本次作业的输入数据路径和输出数据路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        return job.waitForCompletion(true)? 0:1;
    }
}
