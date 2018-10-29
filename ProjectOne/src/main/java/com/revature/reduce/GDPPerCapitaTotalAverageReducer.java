package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class GDPPerCapitaTotalAverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {


	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException {

		double sum = 0.0;
		double totalAverage = 0.0;
		double counter = 0.0;
		
		for (DoubleWritable value : values) {
			sum += value.get();
			counter++;
		}
		
		totalAverage = sum/counter;
		
		context.write(new Text("Average change in GDP per capita since 1960"), new DoubleWritable(totalAverage));
	}
}