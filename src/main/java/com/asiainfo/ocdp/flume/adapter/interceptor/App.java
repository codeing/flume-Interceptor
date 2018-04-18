package com.asiainfo.ocdp.flume.adapter.interceptor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	
    	List<String> list = new ArrayList<String>();
    	list.add("df");
    	list.add(null);
    	System.out.println( "Hello World!  "+list.get(1));
    	String testStr = "11|sfdf|e|2018-04-17 10:40:23.867|34|6|sfdf|e|swds|343";
    	final List<String> valueList = Lists.newArrayList(Splitter.on('|').trimResults().split(testStr));
//    	System.out.println( "Hello World!  "+valueList);
//        System.out.println( "Hello World!  "+valueList.get(3));
        String datetime = valueList.get(3);
    	long timestamp = 0;
    	if (datetime != null && !datetime.equals("")){
    		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            try {
                Date date = sdf.parse(datetime);
                timestamp = date.getTime();
//                System.out.println( "Hello World!1111  "+timestamp);
            } catch (ParseException e) {
                e.printStackTrace();
            }
    	}
    }
}
