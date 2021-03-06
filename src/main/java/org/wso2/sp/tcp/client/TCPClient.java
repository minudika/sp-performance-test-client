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
import org.wso2.sp.tcp.client.util.ClientConstants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
    private static final Attribute.Type[] PATTERN_TYPES = new Attribute.Type[] {
            Attribute.Type.LONG, Attribute.Type.INT, Attribute.Type.LONG, Attribute.Type.DOUBLE,
            Attribute.Type.DOUBLE, Attribute.Type.INT, Attribute.Type.DOUBLE, Attribute.Type.INT,
            Attribute.Type.INT, Attribute.Type.INT, Attribute.Type.INT, Attribute.Type.INT, Attribute.Type.INT,
            Attribute.Type.INT};
    private static final Logger logger = Logger.getLogger(TCPClient.class);
    private static long startTime = 0;

    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final int DEFAULT_BATCH_TIME = 1000;
    private static final String DEFAULT_HOST = "localhost";
    private static long interval;
    private static long duration;
    private static int batchSize;
    private static int scenario;
    private static String host;
    private static int port;

    /**
     * Main method to start the test client
     * Tested scenarios are identified by following ids.
     */
    public static void main(String[] args) {
        try {
            host = System.getProperty(ClientConstants.HOST, ClientConstants.DEFAULT_HOST);
            port = Integer.parseInt(System.getProperty(ClientConstants.PORT, ClientConstants.DEFAULT_PORT));
            batchSize = Integer.parseInt(System.getProperty(ClientConstants.BATCH_SIZE, ClientConstants
                    .DEFAULT_BATCH_SIZE));
            interval = Long.parseLong(System.getProperty(ClientConstants.INTERVAL, ClientConstants
                    .DEFAULT_INTERVAL));
            scenario = Integer.parseInt(System.getProperty(ClientConstants.SCENARIO_TEXT,
                    ClientConstants.DEFAULT_SCENARIO));
            duration = Long.parseLong(System.getProperty(ClientConstants.DURATION, ClientConstants
                    .DEFAULT_DURATION));

            TCPNettyClient tcpNettyClient = new TCPNettyClient();
            tcpNettyClient.connect(host, port);
            switch (scenario) {
                case ClientConstants.SCENARIO_PASSTHROUGH:
                case ClientConstants.SCENARIO_FILTER:
                case ClientConstants.SCENARIO_WINDOW_LARGE:
                case ClientConstants.SCENARIO_WINDOW_SMALL:
                    publishPassthroughMessages(tcpNettyClient);
                    break;
                case ClientConstants.SCENARIO_PATTERNS:
                    publishPatternMessages(tcpNettyClient);
                    break;
                default:
                    publishPersistingMessages(tcpNettyClient);
            }
            tcpNettyClient.disconnect();
            tcpNettyClient.shutdown();
        } catch (ConnectionUnavailableException e) {
            logger.error(String.format("Failed to connect to %s:%d due to %s", host, port, e.getMessage()), e);
        } catch (IOException e) {
            logger.error("Error occured during publishing messages : " + e.getMessage(), e);
        }
    }

    private static void publishPassthroughMessages(TCPNettyClient tcpNettyClient) throws IOException {
        Random rand = new Random();
        long i = 0;
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < duration) {
            long batchStartTime = System.currentTimeMillis();
            List<Event> arrayList = new ArrayList<Event>(batchSize);
            for (int j = 0; j < batchSize; j++) {
                Event event = new Event();
                event.setData(new Object[]{System.currentTimeMillis(), rand.nextFloat()});

                long time = System.currentTimeMillis();
                if (time - batchStartTime <= interval) {
                    arrayList.add(event);
                }
            }
            tcpNettyClient.send(STREAM_NAME, BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[0]), PASSTHROUGH_TYPES).array());
            long currentTime = System.currentTimeMillis();
            if (currentTime - batchStartTime <= interval) {
                try {
                    Thread.sleep(interval - (currentTime - batchStartTime));
                } catch (InterruptedException ex) {
                    logger.error(ex);
                }
            }
        }
        try {
            logger.info("TCP client finished sending events");
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            logger.info("Error occured after publishing events : " + e.getMessage(), e);
        }
    }

    private static void publishPersistingMessages(TCPNettyClient tcpNettyClient) throws IOException {
        Random rand = new Random();
        long i = 0;
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < duration) {
            startTime = System.currentTimeMillis();
            ArrayList<Event> arrayList = new ArrayList<Event>(batchSize);
            for (int j = 0; j < batchSize; j++) {
                Event event = new Event();
                event.setData(new Object[]{System.currentTimeMillis(), rand.nextBoolean(), rand.nextInt(), rand
                        .nextDouble(), rand.nextDouble(), rand.nextFloat(), rand.nextDouble()});

                long time = System.currentTimeMillis();
                if (time - startTime <= interval) {
                    arrayList.add(event);
                }
            }
            tcpNettyClient.send(STREAM_NAME, BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[0]), PERSISTING_TYPES).array());
            long currentTime = System.currentTimeMillis();
            if (currentTime - startTime <= interval) {
                try {
                    Thread.sleep(interval - (currentTime - startTime));
                } catch (InterruptedException e) {
                    logger.info("Error occured while publishing events : " + e.getMessage(), e);
                }
            }
        }
        try {
            logger.info("TCP client finished sending events");
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            logger.info("Error occured after publishing events : " + e.getMessage(), e);
        }
    }

    private static void publishPatternMessages(TCPNettyClient tcpNettyClient) throws IOException {
        Random rand = new Random();
        long i = 0;
        long startTime = System.currentTimeMillis();
        String path = TCPClient.class.getClassLoader().getResource("data.csv").getFile();
        BufferedReader reader = new BufferedReader(new FileReader(path));
        while (System.currentTimeMillis() - startTime < duration) {
            startTime = System.currentTimeMillis();
            ArrayList<Event> arrayList = new ArrayList<Event>(batchSize);
            for (int j = 0; j < batchSize; j++) {
                Event event = new Event();
                String line = reader.readLine();
                String data[];
                if (line != null) {
                    data = line.split(",");
                } else {
                    reader.close();
                    reader = new BufferedReader(new FileReader(path));
                    data = reader.readLine().split(",");
                }

                event.setData(new Object[]{System.currentTimeMillis(), data[0], data[1], data[2], data[3], data[4],
                        data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[12], data[13]});

                long time = System.currentTimeMillis();
                if (time - startTime <= interval) {
                    arrayList.add(event);
                }
            }
            tcpNettyClient.send(STREAM_NAME, BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[0]), PATTERN_TYPES).array());
            long currentTime = System.currentTimeMillis();
            if (currentTime - startTime <= interval) {
                try {
                    Thread.sleep(interval - (currentTime - startTime));
                } catch (InterruptedException e) {
                    logger.info("Error occured while publishing events : " + e.getMessage(), e);
                }
            }
        }
        try {
            logger.info("TCP client finished sending events");
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            logger.info("Error occured after publishing events : " + e.getMessage(), e);
        }
    }

    private static String getCurrentTimestamp() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }
}
