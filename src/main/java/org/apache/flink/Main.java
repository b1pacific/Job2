/*
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

package org.apache.flink;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.KafkaSourceHelper;
import io.github.devlibx.easy.flink.utils.v2.MainTemplateV2;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.flink.utils.v2.config.KafkaSinkConfig;
import io.github.devlibx.easy.flink.utils.v2.config.SourceConfig;
import io.github.devlibx.miscellaneous.flink.drools.DebugSync;
import io.github.devlibx.miscellaneous.flink.job.missedevent.CustomProcessor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.jdbi.v3.core.Jdbi;

import java.io.Serializable;
import java.util.UUID;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class Main implements MainTemplateV2.RunJob<Configuration> {

    void internalRun(StreamExecutionEnvironment env, Configuration configuration, DataStream<StringObjectMap> inputStream, Class<Configuration> aClass) {

        // Make sure we have a good configuration
        configuration.validate();

        // Step 3.1 - Send it to store

        MySQLSink sink = new MySQLSink();

        KeyedStream<StringObjectMap, String> keyedStream = inputStream.keyBy(new KeySelectorImpl(configuration));

        keyedStream.process(new JobProcessor()).addSink(sink);

    }

    @Override
    public void run(StreamExecutionEnvironment env, Configuration configuration, Class<Configuration> aClass) {
        // Filter and process
        SourceConfig sourceConfig = configuration.getSourceByName("mainInput")
                .orElseThrow(() -> new RuntimeException("Did not find source with name=mainInput in config file"));

        internalRun(env, configuration, sourceConfig.getKafkaSourceWithStringObjectMap(env), aClass);
    }

    public static void main(String[] args) throws Exception {
        String jobName = "MissedEventJobV2";
        for (int i = 0; i < args.length; i++) {
            if (Objects.equal(args[i], "--name")) {
                jobName = args[i + 1];
                break;
            }
        }

        Main job = new Main();
        MainTemplateV2<Configuration> template = new MainTemplateV2<>();
        template.main(args, jobName, job, Configuration.class);
    }

    private static final class KeySelectorImpl implements KeySelector<StringObjectMap, String>, Serializable {
        private final String path;

        private KeySelectorImpl(Configuration configuration) {
            this.path = "id";
//			this.path = configuration.getMiscellaneousProperties().getString("key-path");
        }

        @Override
        public String getKey(StringObjectMap value) {
            return "1";
//            if (value == null || Strings.isNullOrEmpty(value.path(path, String.class))) {
//                return UUID.randomUUID().toString();
//            } else {
//                return "1";
//                // return value.path(path, String.class);
//            }
        }
    }
}
