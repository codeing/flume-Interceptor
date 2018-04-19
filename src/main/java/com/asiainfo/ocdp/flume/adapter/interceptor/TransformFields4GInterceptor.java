package com.asiainfo.ocdp.flume.adapter.interceptor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class TransformFields4GInterceptor implements Interceptor{
 
	private static final Logger logger = LoggerFactory.getLogger(TransformFields23GInterceptor.class);
	// 分隔符
	private String separator = "";
	// 列数
	private int rowNumber;
	// key的位置
	private int keyLocation;
   
	private TransformFields4GInterceptor(String separator, int rowNumber, int keyLocation) {
		this.separator = separator;
		this.rowNumber = rowNumber;
		this.keyLocation = keyLocation;
	}
    @Override
	public Event intercept(Event event) {
		Map<String, String> headers = event.getHeaders();
		String body = new String(event.getBody(), Charsets.UTF_8);
		final List<String> valueList = Lists.newArrayList(Splitter.on(separator).trimResults().split(body));
		int keyIndex = keyLocation - 1;
		String keyValue = valueList.get(keyIndex);
		headers.put(Constants.KEY, keyValue);
		event.setHeaders(headers);
		// 时间在第四列 第一列imis
		if (valueList.size() == rowNumber && StringUtils.isNotBlank(keyValue)) {

			StringBuffer sb = new StringBuffer();
			for (String record : valueList) {
				sb.append(record).append(separator);
			}
			String bodyString = sb.substring(0, sb.lastIndexOf(separator));
			event.setBody(bodyString.getBytes());
			return event;

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
        @Override
        public Interceptor build() {
            return new TransformFields4GInterceptor(separator,rowNumber,keyLocation);
        }

        @Override
        public void configure(Context context) {
            this.separator = context.getString(Constants.SEPARATOR).trim();
            this.rowNumber = context.getInteger(Constants.ROWNUMBER).intValue();
            this.keyLocation = context.getInteger(Constants.KEYLOCATION).intValue();
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
