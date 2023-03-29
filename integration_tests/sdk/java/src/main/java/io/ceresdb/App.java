package io.ceresdb;

import io.ceresdb.models.*;
import io.ceresdb.options.CeresDBOptions;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.ceresdb.RouteMode.DIRECT;

public class App {
    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    private static String TABLE = "table_for_java_tests" + System.currentTimeMillis();
    private static String HOST = "localhost";
    private static int PORT = 8831;
    private static CeresDBClient CLIENT;

    static {
        final CeresDBOptions opts = CeresDBOptions.newBuilder(HOST, PORT, DIRECT) // CeresDB default grpc port 8831，use DIRECT RouteMode
                .database("public") // use database for client, can be overridden by the RequestContext in request
                // maximum retry times when write fails
                // (only some error codes will be retried, such as the routing table failure)
                .writeMaxRetries(1)
                // maximum retry times when read fails
                // (only some error codes will be retried, such as the routing table failure)
                .readMaxRetries(1).build();

        CLIENT = new CeresDBClient();
        if (!CLIENT.init(opts)) {
            throw new IllegalStateException("Fail to start CeresDBClient");
        }
    }

    private static void query(long now, boolean addNewColumn) throws Throwable {
        final SqlQueryRequest queryRequest = SqlQueryRequest.newBuilder()
                .forTables(TABLE) // table name is optional. If not provided, SQL parser will parse the `ssql` to get the table name and do the routing automaticly
                .sql("select * from %s where timestamp = %d", TABLE, now)
                .build();
        final CompletableFuture<Result<SqlQueryOk, Err>> qf = CLIENT.sqlQuery(queryRequest);
        final Result<SqlQueryOk, Err> queryResult = qf.get();

        Assert.assertTrue(queryResult.isOk());

        final SqlQueryOk queryOk = queryResult.getOk();
        // TODO: add row equal assert
        LOGGER.warn("result {}", queryOk.getRowList());
        Assert.assertEquals(2, queryOk.getRowCount());
    }

    private static void write(long now, boolean addNewColumn) throws Throwable {
        List<Point> points = new LinkedList<>();
        for (int i = 0; i < 2; i++) {
            Point.PointBuilder pointBuilder = Point.newPointBuilder(TABLE)
                    .setTimestamp(now)
                    .addTag("tag", String.format("tag-%d", i))
                    .addField("value", Value.withInt8(10 + i));
            if (addNewColumn) {
                pointBuilder = pointBuilder
                        .addTag("new-tag", String.format("new-tag-%d", i));
            }

            points.add(pointBuilder.build());
        }
        final CompletableFuture<Result<WriteOk, Err>> wf = CLIENT.write(new WriteRequest(points));
        final Result<WriteOk, Err> writeResult = wf.get();
        Assert.assertTrue(writeResult.isOk());
    }

    private static void checkAutoCreateTable() throws Throwable {
        long now = System.currentTimeMillis();
        write(now, false);
        query(now, false);
    }

    private static void checkAutoAddColumns() throws Throwable {
        long now = System.currentTimeMillis();
        write(now, true);
        query(now, true);
    }

    private static void run() throws Throwable {
        checkAutoCreateTable();
        checkAutoAddColumns();
    }

    public static void main(String[] args) {
        LOGGER.warn("Begin tests, table:{}", TABLE);
        try {
            run();
        } catch (Throwable e) {
            LOGGER.error("Test failed", e);
            System.exit(1);
        }

        LOGGER.warn("Test finish.");
        System.exit(0);
    }
}
