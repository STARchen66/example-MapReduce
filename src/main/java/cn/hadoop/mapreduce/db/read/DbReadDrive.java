package cn.hadoop.mapreduce.db.read;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DbReadDrive {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        DBConfiguration.configureDB(
                conf,
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://localhost:3306/bigdata_mapreduce",
                "root",
                "200202"
        );

        //创建作业的job类
        Job job = Job.getInstance(conf,DbReadDrive.class.getSimpleName());

        //设置本次mr的驱动类
        job.setJarByClass(DbReadDrive.class);

        //设置mapper类
        job.setMapperClass(DbReadMapper.class);
        //设置程序输出的kv类型
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job,new Path("D://bigData//MilitarySize//outputDB"));

        //将reducetask设置为0
        job.setNumReduceTasks(0);

        //设置输入组件
        job.setInputFormatClass(DBInputFormat.class);
        //添加读取数据库相关参数
        DBInputFormat.setInput(
                job,
                GoodsBean.class,
                "SELECT country,activeDuty,paramilitary,reserves,total,pop2022 FROM `army-total-world`",
                "SELECT CHAR_LENGTH(country) FROM `army-total-world`"
        );

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
