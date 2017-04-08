package com.liminala.hiris;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class DateFormatThread extends Thread {

	public static ThreadLocal<DateFormat> dateFormat = new ThreadLocal<DateFormat>();
	
	
	public DateFormatThread(ThreadGroup group, Runnable target, String name) {
		super(group, target, name);
		
			
//		System.out.println("created thread "+name+", df = "+dateFormat.get());

	}
	
}
