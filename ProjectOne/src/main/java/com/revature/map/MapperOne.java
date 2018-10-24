package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperOne extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String lineTrim = line.substring(1, line.length() - 2);
		String[] col = lineTrim.split("\",\"");

		if(col[3].equals("SE.PRM.CMPL.FE.ZS") || col[3].equals("SE.TER.CMPL.FE.ZS")) {

			for(int index = 4; index < col.length; index++) {
				//!col[index].equals("")
				if(col[index].length() > 0) {
					context.write(new Text(col[0]), new DoubleWritable(Double.parseDouble(col[index])));
				}
			}
			
		}

	}
	
}