package org.apache.flink;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.rule.drools.ResultMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Query;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class JobProcessor extends KeyedProcessFunction<String, StringObjectMap, StringObjectMap> {

    Jdbi jdbi;

    public final static String KEYWORD_SEPARATOR = "-";
    public final static String PROCESSOR_STATE = "processor-state";

    public transient MapState<String, Integer> store;

    public JobProcessor() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jdbi = Jdbi.create("jdbc:mysql://localhost:3306/test", "root", "");

        MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>(
                "state" + KEYWORD_SEPARATOR + PROCESSOR_STATE,
                String.class,
                Integer.class
        );

        mapStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(100)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build());

        store = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public void processElement(StringObjectMap value, KeyedProcessFunction<String, StringObjectMap, StringObjectMap>.Context ctx, Collector<StringObjectMap> out) throws Exception {

        log.info(String.valueOf(value));

        log.info("Hello from here");

//        System.out.println("From result " + Objects.equals(value.get("id").getClass().getSimpleName(), "Integer"));
//
//        System.out.println("Receiving " + value.get("mrks"));
//
//        System.out.println("mrks " + value.get("mrks"));
//        System.out.println("id " + value.get("id"));
//        System.out.println("id " + Objects.equals(value.get("id").getClass().getSimpleName(), "Integer"));
//        System.out.println("mrks " + Objects.equals(value.get("mrks").getClass().getSimpleName(), "Integer"));


        if (value.get("mrks") != null && value.get("id") != null && Objects.equals(value.get("id").getClass().getSimpleName(), "String") && Objects.equals(value.get("mrks").getClass().getSimpleName(), "Integer")) {
            jdbi.useHandle(handle -> {

                int prev = 0;

                int id = Integer.parseInt(value.get("id", String.class));

                if (!store.contains(value.get("id", String.class))) {
                    Query query = handle.createQuery("select mrks from tb where id = :userId");
                    query.bind("userId", id);

                    List<Map<String, Object>> results = query.mapToMap().list();

                    System.out.println("Size of results " + results.size());

                    if (results.size() < 1) {
                        handle.createUpdate("insert into tb (id, mrks) values(:userId, 0)").bind("userId", id).execute();
                    } else
                        prev = (int) results.get(0).get("mrks");
                } else {
                    prev = store.get(value.get("id", String.class));
                }

                StringObjectMap res = new StringObjectMap();

                int newMarks = prev;
                newMarks += value.getInt("mrks");

                store.put(value.get("id", String.class), newMarks);

                res.put("mrks", newMarks);
                res.put("id", id);

                out.collect(res);

            });
        } else {
            System.out.println("Id not present");
        }
    }
}
