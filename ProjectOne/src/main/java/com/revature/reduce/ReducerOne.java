package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerOne extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		double sum = 0.0;
		double counter = 0.0;
		double average = 0.0;
		
		for (DoubleWritable value : values) {
			sum += value.get();
			counter++;
		}
		
		average = sum/counter;
		
		if(average < 30) {
			context.write(key, new DoubleWritable(average));
		}
	}
}