package com.linear;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class LinearRegressionReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    //private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	float sum = 0.0f;
    	for (FloatWritable val : values){
    		sum += val.get();
    	}
    	context.write(key, new FloatWritable(sum));
    }
}
