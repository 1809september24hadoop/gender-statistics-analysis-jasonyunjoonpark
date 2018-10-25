package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerThree extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		double difference = 0.0;
		double currentValue = values.iterator().next().get();  
		double nextValue = 0.0;
		double average = 0.0;
		
		while(values.iterator().hasNext()) {
			nextValue = values.iterator().next().get();
			difference = currentValue - nextValue;	
		}
		
		average = difference/nextValue;
		
		context.write(key, new DoubleWritable(average));
	
	}
}