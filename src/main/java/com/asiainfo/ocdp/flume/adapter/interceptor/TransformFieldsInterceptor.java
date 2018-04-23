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
a1.sources.s1.interceptors.i2.type = com.asiainfo.ocdp.flume.adapter.interceptor.TransformFieldsInterceptor$Builder
a1.sources.s1.interceptors.i2.datasourceSeparator = \|
a1.sources.s1.interceptors.i2.needSeparator = \|

a1.sources.s1.interceptors.i2.fieldNumber = 10
a1.sources.s1.interceptors.i2.keyLocation = 1
a1.sources.s1.interceptors.i2.timeLocation = 4
a1.sources.s1.interceptors.i2.dateFormat  = yyyy-MM-dd HH:mm:ss.SSS
a1.sources.s1.interceptors.i2.dataSourceType  = 23G

# Define a kafka channel
a1.channels.c1.type = org.apache.flume.channel.kafka.KafkaChannel
a1.channels.c1.kafka.bootstrap.servers = host-10-1-241-58:6667
a1.channels.c1.kafka.topic = ocspOutput
a1.channels.c1.parseAsFlumeEvent = false
        并发度      执行时间     一个批次
 *
 */
public class TransformFieldsInterceptor implements Interceptor{

	private static final Logger logger = LoggerFactory.getLogger(TransformFieldsInterceptor.class);
	// 数据源分隔符
	private String datasourceSeparator = "";
	// 需要的分隔符
	private String needSeparator = "";
	// 日期格式
	private String dateFormat = "";
	// 数据来源
	private String dataSource = "";
	// 列数
	private int fieldNumber;
	// key的位置
	private int keyLocation;
	// 时间字段位置
	private int timeLocation;
	

	private TransformFieldsInterceptor(String datasourceSeparator,String needSeparator, String dateFormat, String dataSource, int fieldNumber,
			int keyLocation, int timeLocation) {
		this.datasourceSeparator = datasourceSeparator;
		this.needSeparator = datasourceSeparator;
		this.dateFormat = dateFormat;
		this.dataSource = dataSource;
		this.fieldNumber = fieldNumber;
		this.keyLocation = keyLocation;
		this.timeLocation = timeLocation;
	}
    @Override
	public Event intercept(Event event) {
		try {	
			Map<String, String> headers = event.getHeaders();
			String body = new String(event.getBody(), Charsets.UTF_8);
			if (StringUtils.isNotBlank(body)) {
				final List<String> valueList = Lists.newArrayList(Splitter.on(datasourceSeparator).trimResults().split(body));
				// 23G
				if (dataSource.equalsIgnoreCase(Constants.DATASOURCE_DEFAULT_TYPE)) {
					if (keyLocation < 1 || timeLocation < 1 || keyLocation > valueList.size()
							|| timeLocation > valueList.size() || fieldNumber != valueList.size()) {
						logger.warn("event  body is  " + body);
						logger.warn("flume conf keyLocation is " + keyLocation + ", timeLocation is " + timeLocation
								+ ", rowNumber is " + fieldNumber);
						throw new IllegalArgumentException("keyLocation or timeLocation or rowNumber  config error !");
					}
				} else {
					if (keyLocation < 1 || keyLocation > valueList.size() || timeLocation > valueList.size()
							|| fieldNumber != valueList.size()) {
						logger.warn("event  body is  " + body);
						logger.warn("flume conf keyLocation is " + keyLocation + ", rowNumber is " + fieldNumber);
						throw new IllegalArgumentException("keyLocation  or rowNumber  config error !");
					}
				}

				int keyIndex = keyLocation - 1;
				int timeIndex = timeLocation - 1;
				String keyValue = valueList.get(keyIndex);
				if (StringUtils.isNotBlank(keyValue)) {
					headers.put(Constants.KEY, keyValue);
					event.setHeaders(headers);
					if (dataSource.equalsIgnoreCase(Constants.DATASOURCE_DEFAULT_TYPE)) {
						String datetime = valueList.get(timeIndex).trim();
						long timestamp = 0;
						if (StringUtils.isNotBlank(datetime)) {
							SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
							try {
								Date date = sdf.parse(datetime);
								timestamp = date.getTime();
							} catch (ParseException e) {
								logger.warn("event  body is  " + body + " , dateFormat is " + dateFormat);
								logger.warn("failed to parse  23G  procedure startime. Exception follows is ", e);
							}
						}

						if (timestamp != 0) {
							valueList.set(timeIndex, Long.toString(timestamp));
							String bodyString = Joiner.on(needSeparator).join(valueList);
							event.setBody(bodyString.getBytes(Charsets.UTF_8));
							return event;
						}
					} else {
						String bodyString = Joiner.on(needSeparator).join(valueList);
						event.setBody(bodyString.getBytes(Charsets.UTF_8));
						return event;
					}

				}
			}

		} catch (Exception e) {
			logger.warn("Could not intercept event. Exception follows is ", e);
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


	public static class Builder implements Interceptor.Builder {
		// 数据源分隔符
		private String datasourceSeparator = "";
		// 需要的分隔符
		private String needSeparator = "";
		// 日期格式
		private String dateFormat = "";
		// 数据来源
		private String dataSource = "";
		// 列数
		private int fieldNumber;
		// key的位置
		private int keyLocation;
		// time的位置
		private int timeLocation;
        @Override
		public Interceptor build() {
			return new TransformFieldsInterceptor(datasourceSeparator,needSeparator, dateFormat, dataSource, fieldNumber, keyLocation,
					timeLocation);
		}

        @Override
		public void configure(Context context) {
			this.datasourceSeparator = context
					.getString(Constants.DATASOURCE_SEPARATOR, Constants.DATASOURCE_SEPARATOR_DEFAULT).trim();
			this.needSeparator = context.getString(Constants.NEED_SEPARATOR, Constants.NEED_SEPARATOR_DEFAULT).trim();
			this.dateFormat = context.getString(Constants.DATAFORMAT, Constants.DATETIME_WITH_HYPHEN_COLON_MS).trim();
			this.dataSource = context.getString(Constants.DATASOURCE_TYPE, Constants.DATASOURCE_DEFAULT_TYPE).trim();
			this.fieldNumber = context.getInteger(Constants.RIELDNUMBER, Integer.valueOf(Constants.FIELDNUMBER_DEFAULT))
					.intValue();
			this.keyLocation = context.getInteger(Constants.KEYLOCATION, Integer.valueOf(Constants.KEYLOCATION_DEFAULT))
					.intValue();
			this.timeLocation = context
					.getInteger(Constants.TIMELOCATION, Integer.valueOf(Constants.TIMELOCATION_DEFAULT)).intValue();
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
