/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ashishpaliwal.flume.interceptors;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * This is a sample interceptor, which appends Host:Time entries to the header
 * field HostTime
 */
public class HostTimeInterceptor extends AbstractFlumeInterceptor {

    public static final Logger LOGGER = LoggerFactory.getLogger(HostTimeInterceptor.class);

    private final String header;
    private String host;
    public static final String DEFAULT_SEPARATOR = ",";
    public static final String DEFAULT_KEYVAL_SEPARATOR = ":";


    /**
     * Default constructor. Private constructor to avoid being build by outside source
     *
     * @param headerKey Key for the header to be used
     */
    private HostTimeInterceptor(String headerKey) {
        header = headerKey;
        try {
            host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOGGER.warn("Unable to get Host address", e);
        }
    }

    @Override
    public void initialize() {
        // NOOP
    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();

        String headerValue = headers.get(header);
        if(headerValue != null) {
            headerValue += DEFAULT_SEPARATOR;
        } else {
            headerValue = "";
        }
        headerValue += host + DEFAULT_KEYVAL_SEPARATOR + System.currentTimeMillis();
        headers.put(header, headerValue);

        return event;
    }

    @Override
    public void close() {
        // NOOP
    }

    public static class Builder implements Interceptor.Builder {

        private String headerkey = "HostTime";

        @Override
        public Interceptor build() {
            return new HostTimeInterceptor(headerkey);
        }

        @Override
        public void configure(Context context) {
            headerkey = context.getString("key");
        }
    }
}
