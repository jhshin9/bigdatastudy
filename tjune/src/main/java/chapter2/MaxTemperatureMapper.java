package chapter2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by tjune on 2016. 10. 26..
 * mapper<keyin, valuein, keyout, valueout>
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15, 19);
        int airTemperature;

        airTemperature = line.charAt(87) == '+' ? Integer.parseInt(line.substring(88, 92)) : Integer.parseInt(line.substring(87, 92));

        String quality = line.substring(92,93);
        if(airTemperature!=MISSING && quality.matches("[01459]")){
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}
