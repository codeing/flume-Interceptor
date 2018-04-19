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
 
    private static final Logger logger = LoggerFactory.getLogger(TransformFields23GInterceptor.class);
    // 分隔符
	private String separator = "";
	// 列数
	private int rowNumber;
	// key的位置
	private int keyLocation;
   
	private int timeLocation;

	private TransformFields23GInterceptor(String separator, int rowNumber, int keyLocation,int timeLocation) {
		this.separator = separator;
		this.rowNumber = rowNumber;
		this.keyLocation = keyLocation;
		this.timeLocation = timeLocation;
	}
    @Override
	public Event intercept(Event event) {	
		Map<String, String> headers = event.getHeaders();
		String body = new String(event.getBody(), Charsets.UTF_8);
		final List<String> valueList = Lists.newArrayList(Splitter.on(separator).trimResults().split(body));
		if (keyLocation < 1 || timeLocation< 1){
			logger.error("key or time index config error !");
			return null;
		}
		
		int keyIndex = keyLocation - 1;
		int timeIndex = timeLocation - 1;
		String keyValue = valueList.get(keyIndex);
		headers.put(Constants.KEY, keyValue);
		event.setHeaders(headers);
		if (valueList.size() == rowNumber && StringUtils.isNotBlank(keyValue)) {
			String datetime = valueList.get(timeIndex).trim();
			long timestamp = 0;
			if (StringUtils.isNotBlank(datetime)) {
				SimpleDateFormat sdf = new SimpleDateFormat(Constants.DATETIME_WITH_HYPHEN_COLON_MS);
				try {
					Date date = sdf.parse(datetime);
					timestamp = date.getTime();
				} catch (ParseException e) {
					logger.error("failed to analysis  procedure Start Time");
					e.printStackTrace();
				}
			}
			// 时间戳字段格式不符合 过滤掉
			if (timestamp != 0) {
				valueList.remove(timeIndex);
				valueList.add(timeIndex, Long.toString(timestamp));
				StringBuffer sb = new StringBuffer();
				for (String record : valueList) {
					sb.append(record).append(separator);
				}
				String bodyString = sb.substring(0, sb.lastIndexOf(separator));
				event.setBody(bodyString.getBytes());
				return event;
			}
		}
		return null;
	}

    @Override
    public List<Event> intercept(List<Event> events) {
    	List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());  
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            if (interceptedEvent != null) {
                intercepted.add(interceptedEvent);
            }
        }
        return intercepted;
    }


    public static class Builder implements Interceptor.Builder{
    	// 分隔符
    	private String separator = "";
    	// 列数
    	private int rowNumber;
    	// key的位置
    	private int keyLocation;
    	// time的位置
    	private int timeLocation;
        @Override
        public Interceptor build() {
            return new TransformFields23GInterceptor(separator,rowNumber,keyLocation,timeLocation);
        }

        @Override
        public void configure(Context context) {
             this.separator = context.getString(Constants.SEPARATOR).trim();
             this.rowNumber = context.getInteger(Constants.ROWNUMBER).intValue();
             this.keyLocation = context.getInteger(Constants.KEYLOCATION).intValue();
             this.timeLocation = context.getInteger(Constants.TIMELOCATION).intValue();
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
