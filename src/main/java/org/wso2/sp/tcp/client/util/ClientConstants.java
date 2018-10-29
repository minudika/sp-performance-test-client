package org.wso2.sp.tcp.client.util;

/**
 * Constants
 */
public class ClientConstants {
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String BATCH_SIZE = "batch.size";
    public static final String INTERVAL = "interval";
    public static final String DURATION = "duration";
    public static final String THREAD_COUNT = "thread.count";
    public static final String SCENARIO_TEXT = "scenario";
    public static final String PERSISTENCE_ENABLED = "persistence.enabled";

    public static final String DEFAULT_HOST = "localhost";
    public static final String DEFAULT_PORT = "9892";
    public static final String DEFAULT_BATCH_SIZE = "1000";
    public static final String DEFAULT_THREAD_COUNT = "1";
    public static final String DEFAULT_INTERVAL = "1000";
    public static final String DEFAULT_DURATION = "2400000000";
    public static final String DEFAULT_SCENARIO = "1";

    public static final String FILTER = "filter"; //1
    public static final String PARTITIONS = "partitions"; //2
    public static final String PATTERNS = "patterns";
    public static final String PERSISTENCE_MYSQL = "persistence.mysql";
    public static final String PERSISTENCE_MSSQL = "persistence.mssql";
    public static final String PERSISTENCE_ORACLE = "persistence.oracle";
    public static final String PASSTHROUGH = "passthrough";
    public static final String WINDOW_LARGE = "large.window";
    public static final String WINDOW_SMALL = "small.window";

    public static final int SCENARIO_PASSTHROUGH = 1;
    public static final int SCENARIO_FILTER = 2;
    public static final int SCENARIO_PATTERNS = 3;
    public static final int SCENARIO_PARTITIONS = 4;
    public static final int SCENARIO_WINDOW_LARGE = 5;
    public static final int SCENARIO_WINDOW_SMALL = 6;
    public static final int SCENARIO_PERSISTENCE_MYSQL_INSERT = 7;
    public static final int SCENARIO_PERSISTENCE_MYSQL_UPDATE = 8;
    public static final int SCENARIO_PERSISTENCE_MSSQL_INSERT = 9;
    public static final int SCENARIO_PERSISTENCE_MSSQL_UPDATE = 10;
    public static final int SCENARIO_PERSISTENCE_ORACLE_INSERT = 11;
    public static final int SCENARIO_PERSISTENCE_ORACLE_UPDATE = 12;

}
