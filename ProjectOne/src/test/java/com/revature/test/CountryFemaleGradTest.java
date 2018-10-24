package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.MapperOne;
import com.revature.reduce.ReducerOne;



public class CountryFemaleGradTest {


	  private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	  private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	  
	  
	  
	
}

