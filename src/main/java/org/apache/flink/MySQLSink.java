package org.apache.flink;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MySQLSink extends RichSinkFunction<StringObjectMap> {

    private Jdbi jdbi;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jdbi = Jdbi.create("jdbc:mysql://localhost:3306/test", "root", "");
    }

    @Override
    public void invoke(StringObjectMap value, SinkFunction.Context context) throws Exception {
        System.out.println("Again here " + value.get("mrks").getClass().getSimpleName());
        super.invoke(value, context);
        jdbi.useHandle(handle -> {
            Map<String, Integer> params = new HashMap<>();
            params.put("newMarks", value.getInt("mrks"));
            params.put("userId", value.getInt("id"));

            handle.createUpdate("update tb set mrks = :newMarks where id = :userId").bindMap(params).execute();
        });
    }
}
