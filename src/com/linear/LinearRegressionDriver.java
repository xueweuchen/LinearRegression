package com.linear;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LinearRegressionDriver {

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "127.0.0.1:54311");
		Job job = new Job(conf, "Linear Regression!");
		job.setJarByClass(LinearRegressionDriver.class);
		job.setMapperClass(LinearRegressionMapper.class);
		job.setReducerClass(LinearRegressionReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(FloatWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);
	    
	    String localSrcFile = "/home/hduser/LinearRegression/samples";
	    String localDstFile = "/home/hduser/LinearRegression/linear";
	    String hdfsSrcFile = "temp_src";
	    String hdfsDstFile = "temp_dst";
	    String hdfsfinalFile = "line";
	    HDFS_File file = new HDFS_File();
	    file.PutFile(conf, localSrcFile, hdfsSrcFile);
	    FileInputFormat.addInputPath(job, new Path(hdfsSrcFile));
	    FileOutputFormat.setOutputPath(job, new Path(hdfsDstFile));
	   
	    int ret = (job.waitForCompletion(true) ? 0 : 1);
		
	    float x = 0, y = 0, xy = 0, xx = 0;
		float count = 0;
		float a,b;
		
		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream param = hdfs.open(new Path(hdfsDstFile + "/part-r-00000"));
	    BufferedReader paramBuffer = new BufferedReader(new InputStreamReader(param));
	    String currentLine;
	    while((currentLine = paramBuffer.readLine()) != null){
	    	String[] part = currentLine.split("\t");
	    	if (part[0].equals("x")){
	    		x = Float.parseFloat(part[1]);
	    	}else if (part[0].equals("y")){
	    		y = Float.parseFloat(part[1]);
	    	}else if (part[0].equals("xx")){
	    		xx = Float.parseFloat(part[1]);
	    	}else if (part[0].equals("xy")){
	    		xy = Float.parseFloat(part[1]);
	    	}else {
	    		count = Float.parseFloat(part[1]);
	    	}
	    }
	    paramBuffer.close();
	    a = (count * xy - x * y)/(count * xx - x * x);
	    b = (y - a * x) / count;
	    System.out.println(a);
	    System.out.println(b);
	    
	    FSDataOutputStream paramOut = hdfs.create(new Path(hdfsfinalFile));
	    BufferedWriter paramOutBuffer = new BufferedWriter(new OutputStreamWriter(paramOut));
	    paramOutBuffer.write(new String("a\t" + a + "\n"));
	    paramOutBuffer.write(new String("b\t" + b + "\n"));
	    paramOutBuffer.flush();
	    paramOutBuffer.close();
	    
	    file.GetFile(conf, hdfsfinalFile, localDstFile);
	    file.DelFile(conf, hdfsSrcFile, true);
	    file.DelFile(conf, hdfsDstFile, true);
	}

}
