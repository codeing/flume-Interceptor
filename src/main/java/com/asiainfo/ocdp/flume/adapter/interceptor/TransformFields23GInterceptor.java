package com.asiainfo.ocdp.flume.adapter.interceptor;


import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TransformFields23GInterceptor implements Interceptor{
 
    private static final Logger logger = LoggerFactory
            .getLogger(TransformFields23GInterceptor.class);

    private TransformFields23GInterceptor() {
       
    }
    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charsets.UTF_8);
        StringBuffer  sb = new StringBuffer();
        final List<String> valueList = Lists.newArrayList(Splitter.on('|').trimResults().split(body));
        String imis = valueList.get(0);
        // 时间在第四列  第一类imis
        if(valueList.size() == 10 && StringUtils.isNotEmpty(imis)){
        	String datetime = valueList.get(3);
        	long timestamp = 0;
        	if (datetime != null && !datetime.equals("")){
        		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                try {
                    Date date = sdf.parse(datetime);
                    timestamp = date.getTime();
                } catch (ParseException e) {
                	logger.error("failed to analysis  procedure Start Time");
                    e.printStackTrace();
                }
        	}
        	// 时间戳字段格式不符合 过滤掉
        	if (timestamp != 0){
        		sb.append(valueList.get(0)).append("|").append(valueList.get(1)).append("|")
            	.append(valueList.get(2)).append("|").append(timestamp).append("|")
            	.append(valueList.get(4)).append("|").append(valueList.get(5)).append("|")
            	.append(valueList.get(6)).append("|").append(valueList.get(7)).append("|")
            	.append(valueList.get(8)).append("|").append(valueList.get(9));
            	event.setBody(sb.toString().getBytes());
            	return event;
        	}
        }
      return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> intercepted = new ArrayList();
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            if (interceptedEvent != null) {
                intercepted.add(interceptedEvent);
            }
        }
        return intercepted;
    }


    public static class Builder implements Interceptor.Builder{
  
        @Override
        public Interceptor build() {
            return new TransformFields23GInterceptor();
        }

        @Override
        public void configure(Context context) {
         
        }
    }

    @Override
    public void close() {
        // NO-OP...
    }

    @Override
    public void initialize() {
        // NO-OP...
    }
}
