package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperOne extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String lineTrim = line.substring(1, line.length() - 1);
		String[] col = lineTrim.split("\",\"");
		
		if(col[3] == "SE.PRM.CMPL.FE.ZS" || col[3] == "SE.TER.CMPL.FE.ZS") {
			for(int index = 4; index <= 60; index++) {
				if(col[0].length() > 0) {
					context.write(new Text(col[0]), new DoubleWritable(Double.parseDouble(col[index])));
				}
				
			}
		}
		

//		for (String word : line.split("\\W+")) {
//			if (word.length() > 0) {
//
//				context.write(new Text(word), new IntWritable(1));
//			}
		}

}