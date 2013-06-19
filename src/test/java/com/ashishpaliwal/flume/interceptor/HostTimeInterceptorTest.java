package com.ashishpaliwal.flume.interceptor;

import com.google.common.base.Charsets;
import junit.framework.Assert;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class HostTimeInterceptorTest {

    @Test
    public void testIntercept() throws Exception {
        Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.ashishpaliwal.flume.interceptors.HostTimeInterceptor$Builder");
        Interceptor interceptor = builder.build();
        System.out.println(interceptor);
        Event eventBeforeIntercept = EventBuilder.withBody("test event".getBytes());
        System.out.println(eventBeforeIntercept);
        Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
        Assert.assertNotNull(eventAfterIntercept.getHeaders().get("HostTime"));
        System.out.println(eventAfterIntercept);
        System.out.println(interceptor.intercept(eventAfterIntercept));
    }

}
