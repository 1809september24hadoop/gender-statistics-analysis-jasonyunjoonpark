package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import static com.revature.util.CodeUtil.*;
import static com.revature.util.ColumnIndexUtil.*;
/*
 * Maps countries to 1960 through 2016 GDP Per Capita for each year
 */

public class GDPMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String lineTrim = line.substring(1, line.length() - 2);
		String[] col = lineTrim.split("\",\"");

		String country = col[COUNTRY_INDEX];
		
		
		if(col[CODE_COLUMN_INDEX].equals(GDP_PER_CAPITA_CODE)) {
			for(int index = _1960_COLUMN_INDEX; index < col.length; index++) {
				if(!col[_1960_COLUMN_INDEX].equals("") && !col[col.length - 1].equals("") && !col[_1960_COLUMN_INDEX].equals("0") && !col[col.length - 1].equals("")) {
					
					if(index == _1960_COLUMN_INDEX){
						context.write(new Text(country), new DoubleWritable(Double.parseDouble(col[index])));
					} else if(index == col.length - 1) {
						context.write(new Text(country), new DoubleWritable(Double.parseDouble(col[index])));
					}
					
				}
			}
		}

	}
	
}