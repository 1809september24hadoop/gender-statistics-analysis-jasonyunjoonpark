package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import static com.revature.util.CodeUtil.*;
import static com.revature.util.ColumnIndexUtil.*;

/*
 * Maps Country (USA) to Net % of Female Primary School Enrollment from 2000 to 2016
 */

public class MapperTwo extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String lineTrim = line.substring(1, line.length() - 2);
		String[] col = lineTrim.split("\",\"");

		if(col[COUNTRY_CODE_INDEX].equals(USA_CODE) && col[CODE_COLUMN_INDEX].equals(F_TER_ENROLLMENT_CODE)) {		
				for(int index = 44; index < col.length; index++) {
					if(!col[index].equals("")) {
						context.write(new Text(col[COUNTRY_INDEX]), new DoubleWritable(Double.parseDouble(col[index])));
					}
				}
		} 



	}
		
}
