package com.revature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.revature.map.MapperThree;
import com.revature.reduce.ReducerThree;
import com.revature.reduce.ReducerThreeCombiner;

public class ProblemThreeCombiner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: Problem Three Driver <input dir> <output dir>\n");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJarByClass(ProblemThreeCombiner.class);
		job.setJobName("Problem Three Driver");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MapperThree.class);
		job.setReducerClass(ReducerThree.class);

		/*
		 * Set Intermediate Reducer
		 */
		job.setCombinerClass(ReducerThreeCombiner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		if (job.getCombinerClass() == null) {
			throw new Exception("Combiner not set");
		}

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}


	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new ProblemThreeCombiner(), args);
		System.exit(exitCode);
	}
}