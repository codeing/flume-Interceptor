package com.asiainfo.ocdp.flume.adapter.interceptor;


import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
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
/** 
 
a1.channels = c1
a1.sources = s1
a1.sinks =k1

a1.sources.s1.type = org.apache.flume.source.kafka.KafkaSource
a1.sources.s1.channels = c1
a1.sources.s1.batchSize = 5000
a1.sources.s1.kafka.bootstrap.servers = host-10-1-241-58:6667
a1.sources.s1.zookeeperConnect=host-10-1-241-58:2181
a1.sources.s1.topic = ocspIn
a1.sources.s1.kafka.consumer.group.id = test

a1.sources.s1.interceptors = i2
a1.sources.s1.interceptors.i2.type = com.asiainfo.ocdp.flume.adapter.interceptor.TransformFields23GInterceptor$Builder
a1.sources.s1.interceptors.i2.separator = \|
a1.sources.s1.interceptors.i2.rowNumber = 10
a1.sources.s1.interceptors.i2.keyLocation = 1
a1.sources.s1.interceptors.i2.timeLocation = 4
a1.sources.s1.interceptors.i2.dateFormat  = 4
a1.sources.s1.interceptors.i2.dataSource  = 23G

# Define a kafka channel
a1.channels.c1.type = org.apache.flume.channel.kafka.KafkaChannel
a1.channels.c1.kafka.bootstrap.servers = host-10-1-241-58:6667
a1.channels.c1.kafka.topic = ocspOutput
a1.channels.c1.parseAsFlumeEvent = false
 *
 */
public class TransformFieldsInterceptor implements Interceptor{
 
    private static final Logger logger = LoggerFactory.getLogger(TransformFieldsInterceptor.class);
    // 分隔符
	private String separator = "";
	// 日期格式
	private String dateFormat = "";
	// 数据来源
	private String dataSource = "";
	// 列数
	private int rowNumber;
	// key的位置
	private int keyLocation;
   
	private int timeLocation;
	

	private TransformFieldsInterceptor(String separator,String dateFormat,String dataSource, int rowNumber, int keyLocation,int timeLocation) {
		this.separator = separator;
		this.dateFormat = dateFormat;
		this.dataSource = dataSource;
		this.rowNumber = rowNumber;
		this.keyLocation = keyLocation;
		this.timeLocation = timeLocation;
	}
    @Override
	public Event intercept(Event event) {	
    	try{
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
    			if(dataSource.equalsIgnoreCase(Constants.DATASOURCE_DEFAULT)){
    				String datetime = valueList.get(timeIndex).trim();
    				long timestamp = 0;
    				if (StringUtils.isNotBlank(datetime)) {
    					SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
    					try {
    						Date date = sdf.parse(datetime);
    						timestamp = date.getTime();
    					} catch (ParseException e) {
    						logger.error("failed to parse  23G dataSource procedure startime. Exception follows. {} ",e);
    					}
    				}
    				// 时间戳字段格式不符合 过滤掉
    				if (timestamp != 0) {
    					valueList.set(timeIndex, Long.toString(timestamp));
    					String bodyString = Joiner.on(separator).join(valueList);;
    					event.setBody(bodyString.getBytes(Charsets.UTF_8));
    					return event;
    				}
    			}
    		}
    	}catch (Exception e) {
    	   logger.warn("Could not intercept event. Exception follows. {} ", e);
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
    	// 日期格式
    	private String dateFormat = "";
    	// 数据来源
    	private String dataSource = "";
    	// 列数
    	private int rowNumber;
    	// key的位置
    	private int keyLocation;
    	// time的位置
    	private int timeLocation;
        @Override
        public Interceptor build() {
            return new TransformFieldsInterceptor(separator,dateFormat,dataSource,rowNumber,keyLocation,timeLocation);
        }

        @Override
        public void configure(Context context) {
             this.separator = context.getString(Constants.SEPARATOR,Constants.SEPARATOR_SYMBOL).trim();
             this.dateFormat = context.getString(Constants.DATAFORMAT,Constants.DATETIME_WITH_HYPHEN_COLON_MS).trim();
             this.dataSource = context.getString(Constants.DATASOURCE,Constants.DATASOURCE_DEFAULT).trim();
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
