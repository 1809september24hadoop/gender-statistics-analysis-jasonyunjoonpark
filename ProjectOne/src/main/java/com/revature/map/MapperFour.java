package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperFour extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String lineTrim = line.substring(1, line.length() - 2);
		String[] col = lineTrim.split("\",\"");

		String country = col[0];
		
		if(col[3].equals("SL.EMP.TOTL.SP.FE.ZS") && !col[0].equals("World")) {
			for(int index = 44; index < col.length; index++) {
				if(!col[44].equals("") && !col[col.length - 1].equals("") && !col[44].equals("0") && !col[col.length - 1].equals("0")) {
					
					if(index == 44){
						context.write(new Text(country), new DoubleWritable(Double.parseDouble(col[index])));
					} else if(index == col.length - 1) {
						context.write(new Text(country), new DoubleWritable(Double.parseDouble(col[index])));
					}
					
				}
			}
		}


	}
	
}
	

