/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.sp.tcp.client;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyClient;
import org.wso2.extension.siddhi.map.binary.sinkmapper.BinaryEventConverter;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

/**
 * Test client for TCP source
 */
public class TCPClient {
    private static final long EVENT_COUNT = 100000000L;
    private static final String STREAM_NAME = "inputStream"; //TCP_Benchmark
    private static final Attribute.Type[] PASSTHROUGH_TYPES = new Attribute.Type[]{Attribute.Type.LONG,
            Attribute.Type.FLOAT};
    private static final Attribute.Type[] PERSISTING_TYPES = new Attribute.Type[]{
            Attribute.Type.LONG, Attribute.Type.BOOL, Attribute.Type.INT, Attribute.Type.DOUBLE,
            Attribute.Type.DOUBLE, Attribute.Type.FLOAT, Attribute.Type.DOUBLE};
    private static final Logger logger = Logger.getLogger(TCPClient.class);
    private static final int BATCH_SIZE = 1000;
    private static final int BATCH_TIME = 1000;
    private static long startTime = 0;

    /**
     * Main method to start the test client
     */
    public static void main(String[] args) {
        try {
            String host = args[0];
            String port = args[1];
            boolean persistenceEnabled = Boolean.parseBoolean(args[2]);

            logger.info("Host : " + host);
            logger.info("Port : " + port);
            logger.info("persistenceEnabled : " + persistenceEnabled);

            TCPNettyClient tcpNettyClient = new TCPNettyClient();
            tcpNettyClient.connect(host, Integer.parseInt(port));
            logger.info("TCP client connected");
            if (!persistenceEnabled) {
                publishPassthroughMessages(tcpNettyClient);
            } else {
                publishPersistingMessages(tcpNettyClient);
            }

            tcpNettyClient.disconnect();
            tcpNettyClient.shutdown();
        } catch (ConnectionUnavailableException ex) {
            logger.error(ex);
        } catch (IOException e) {
            logger.error(e);
        }
    }

    private static void publishPassthroughMessages(TCPNettyClient tcpNettyClient) throws IOException {
        Random rand = new Random();
        long i = 0;
        for (; i < EVENT_COUNT; i += BATCH_SIZE) {
            startTime = System.currentTimeMillis();
            ArrayList<Event> arrayList = new ArrayList<Event>(BATCH_SIZE);
            for (int j = 0; j < BATCH_SIZE; j++) {
                Event event = new Event();
                event.setData(new Object[]{System.currentTimeMillis(), rand.nextFloat()});

                long time = System.currentTimeMillis();
                if (time - startTime <= BATCH_TIME) {
                    arrayList.add(event);
                }
            }
            tcpNettyClient.send(STREAM_NAME, BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[0]), PASSTHROUGH_TYPES).array());
            long currentTime = System.currentTimeMillis();
            if (currentTime - startTime <= BATCH_TIME) {
                try {
                    Thread.sleep(BATCH_TIME - (currentTime - startTime));
                } catch (InterruptedException ex) {
                    logger.error(ex);
                }
            }
        }
        try {
            logger.info("TCP client finished sending events");
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
    }

    private static void publishPersistingMessages(TCPNettyClient tcpNettyClient) throws IOException {
        Random rand = new Random();
        long i = 0;
        for (; i < EVENT_COUNT; i += BATCH_SIZE) {
            startTime = System.currentTimeMillis();
            ArrayList<Event> arrayList = new ArrayList<Event>(BATCH_SIZE);
            for (int j = 0; j < BATCH_SIZE; j++) {
                Event event = new Event();
                event.setData(new Object[]{System.currentTimeMillis(), rand.nextBoolean(), rand.nextInt(), rand
                        .nextDouble(), rand.nextDouble(), rand.nextFloat(), rand.nextDouble()});

                long time = System.currentTimeMillis();
                if (time - startTime <= BATCH_TIME) {
                    arrayList.add(event);
                }
            }
            tcpNettyClient.send(STREAM_NAME, BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[0]), PERSISTING_TYPES).array());
            long currentTime = System.currentTimeMillis();
            if (currentTime - startTime <= BATCH_TIME) {
                try {
                    Thread.sleep(BATCH_TIME - (currentTime - startTime));
                } catch (InterruptedException ex) {
                    logger.error(ex);
                }
            }
        }
        try {
            logger.info("TCP client finished sending events");
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
    }

    private static String getCurrentTimestamp() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }
}
