package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import static com.revature.util.CodeUtil.*;
import static com.revature.util.ColumnIndexUtil.*;

/*
 * Maps Country to female tertiary school graduation rates from 1960 to 2016
 */

public class MapperOne extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String lineTrim = line.substring(1, line.length() - 2);
		String[] col = lineTrim.split("\",\"");

		
		if(col[CODE_COLUMN_INDEX].equals(F_GROSS_TER_GRAD_RATIO_CODE)) {
			for(int index = _1960_COLUMN_INDEX; index < col.length; index++) {
				if(col[index].length() > 0 && !col[index].equals("")) {
					context.write(new Text(col[COUNTRY_INDEX]), new DoubleWritable(Double.parseDouble(col[index])));
				}
			}
		}
	

	}
	
}