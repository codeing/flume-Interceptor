package com.asiainfo.ocdp.flume.adapter.interceptor;

import java.util.ArrayList;
import java.util.List;

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
 
    private static final Logger logger = LoggerFactory
            .getLogger(TransformFields23GInterceptor.class);

    private TransformFields4GInterceptor() {
       
    }
    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charsets.UTF_8);
        final List<String> valueList = Lists.newArrayList(Splitter.on('|').trimResults().split(body));
        String imis = valueList.get(0);
        // 4G 
        if(valueList.size() == 10 && StringUtils.isNotEmpty(imis)){
        	return event;
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
            return new TransformFields4GInterceptor();
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
