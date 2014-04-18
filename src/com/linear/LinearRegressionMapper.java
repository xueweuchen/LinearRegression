package com.linear;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class LinearRegressionMapper  extends Mapper<Object, Text, Text, FloatWritable>{
    
	  public void map(Object key, Text value, Context context
	                  ) throws IOException, InterruptedException {
	    String[] part = value.toString().split(":");
	    float x = Float.parseFloat(part[0]);
	    float y = Float.parseFloat(part[1]);
	    context.write(new Text("x"), new FloatWritable(x));
	    context.write(new Text("y"), new FloatWritable(y));
	    context.write(new Text("xy"), new FloatWritable(x*y));
	    context.write(new Text("xx"), new FloatWritable(x*x));
	    context.write(new Text("C"), new FloatWritable(1.0f));
	  }
  }