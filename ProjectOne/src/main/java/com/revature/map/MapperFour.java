package com.revature.map;

import static com.revature.util.CodeUtil.M_EMP_TO_POP_RATIO_CODE;
import static com.revature.util.CodeUtil.WORLD_CODE;
import static com.revature.util.ColumnIndexUtil.CODE_COLUMN_INDEX;
import static com.revature.util.ColumnIndexUtil.COUNTRY_INDEX;
import static com.revature.util.ColumnIndexUtil._2000_COLUMN_INDEX;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import static com.revature.util.CodeUtil.*;
import static com.revature.util.ColumnIndexUtil.*;

public class MapperFour extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String lineTrim = line.substring(1, line.length() - 2);
		String[] col = lineTrim.split("\",\"");

		String country = col[COUNTRY_INDEX];
		
		
		if(col[CODE_COLUMN_INDEX].equals(F_EMP_TO_POP_RATIO_CODE) && col[COUNTRY_INDEX].equals(WORLD_CODE)) {
			for(int index = _2000_COLUMN_INDEX; index < col.length; index++) {
				if(!col[_2000_COLUMN_INDEX].equals("") && !col[col.length - 1].equals("") && !col[_2000_COLUMN_INDEX].equals("0") && !col[col.length - 1].equals("")) {
					
					if(index == _2000_COLUMN_INDEX){
						context.write(new Text(country), new DoubleWritable(Double.parseDouble(col[index])));
					} else if(index == col.length - 1) {
						context.write(new Text(country), new DoubleWritable(Double.parseDouble(col[index])));
					}
					
				}
			}
		}

	}
	
}
	


