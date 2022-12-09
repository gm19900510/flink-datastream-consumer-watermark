package com.gm.example;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.flink.legacy.RocketMQConfig;
import org.apache.rocketmq.flink.legacy.RocketMQSink;
import org.apache.rocketmq.flink.legacy.RocketMQSourceFunction;
import org.apache.rocketmq.flink.legacy.common.serialization.SimpleTupleDeserializationSchema;
import org.apache.rocketmq.flink.legacy.function.SinkMapFunction;
import org.apache.rocketmq.flink.legacy.function.SourceMapFunction;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import static org.apache.rocketmq.flink.legacy.RocketMQConfig.CONSUMER_OFFSET_LATEST;
import static org.apache.rocketmq.flink.legacy.RocketMQConfig.DEFAULT_CONSUMER_TAG;

public class RocketMQFlinkWindowExample {

    /**
     * Source Config
     *
     * @return properties
     */
    private static Properties getConsumerProps() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(
                RocketMQConfig.NAME_SERVER_ADDR,
                "120.48.81.173:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "GID_SIMPLE_CONSUMER");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "SOURCE_TOPIC");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TAG, DEFAULT_CONSUMER_TAG);
        consumerProps.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_LATEST);
        return consumerProps;
    }

    /**
     * Sink Config
     *
     * @return properties
     */
    private static Properties getProducerProps() {
        Properties producerProps = new Properties();
        producerProps.setProperty(
                RocketMQConfig.NAME_SERVER_ADDR,
                "120.48.81.173:9876");
        producerProps.setProperty(RocketMQConfig.PRODUCER_GROUP, "flink_produce_test");
        return producerProps;
    }

    public static void main(String[] args) throws Exception {

        //final ParameterTool params = ParameterTool.fromArgs(args);

        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8082);

        // for local
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        // for cluster
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.getConfig().setGlobalJobParameters(params);
        env.setStateBackend(new MemoryStateBackend());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start a checkpoint every 10s
        env.enableCheckpointing(10000);
        // advanced options:
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties consumerProps = getConsumerProps();
        Properties producerProps = getProducerProps();

        SimpleTupleDeserializationSchema schema = new SimpleTupleDeserializationSchema();

        DataStreamSource<Tuple2<String, String>> source =
                env.addSource(new RocketMQSourceFunction<>(schema, consumerProps))
                        .setParallelism(2);
        source.name("source");

        source.process(new SourceMapFunction())
                .process(new SinkMapFunction("SINK_TOPIC", "*"))
                .addSink(
                        new RocketMQSink(producerProps)
                                .withBatchFlushOnCheckpoint(true)
                                .withBatchSize(32)
                                .withAsync(true))
                .setParallelism(2).name("sink");

        DataStream<JSONObject> dataStream = source.map(lines -> JSONObject.parseObject(lines.f1)).name("to json");

        /**
         DataStream<JSONObject> withTimestampsAndWatermarks = dataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness
         (Duration.ofSeconds(30)).withIdleness(Duration.ofSeconds(60)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>(){
             @Override
             public long extractTimestamp(JSONObject element, long recordTimestamp) {
                 return element.getLong("eventTime");
             }
         }).withIdleness(Duration.ofSeconds(60)));
        **/

        /**
        DataStream<JSONObject> withTimestampsAndWatermarks = dataStream.assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator());
        **/

        /**
        com.gm.example.Window window = new Window("eventTime",5000L);
        DataStream<JSONObject> withTimestampsAndWatermarks = dataStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(new EventWaterMark(window)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>(){
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("eventTime");
                    }
                }).withIdleness(Duration.ofSeconds(60)));
         **/

        DataStream<JSONObject> withTimestampsAndWatermarks = dataStream.assignTimestampsAndWatermarks(new WatermarkFunction().withIdleness(Duration.ofSeconds(60)));

        /**
        DataStream<List<JSONObject>> result = withTimestampsAndWatermarks.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(15L))).apply(new AllWindowFunction<JSONObject, List<JSONObject>, TimeWindow>() {

            @Override
            public void apply(TimeWindow timeWindow, Iterable<JSONObject> iterable, Collector<List<JSONObject>> out) throws Exception {
                List<JSONObject> events = Lists.newArrayList(iterable);

                if(events.size() > 0) {
                    SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = new Date();
                    System.out.println(dateFormat.format(date) + " 15秒内总共收到的条数：" + events.size());
                    for(JSONObject event:events){
                        System.out.println(event);
                    }
                    out.collect(events);
                }

            }
        });
        **/

        withTimestampsAndWatermarks.print("原始接受数据");

        DataStream<List<JSONObject>> result = withTimestampsAndWatermarks.timeWindowAll(Time.seconds(15L)).apply(new AllWindowFunction<JSONObject, List<JSONObject>, TimeWindow>() {

            @Override
            public void apply(TimeWindow timeWindow, Iterable<JSONObject> iterable, Collector<List<JSONObject>> out) throws Exception {
                List<JSONObject> events = Lists.newArrayList(iterable);

                if(events.size() > 0) {
                    SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = new Date();
                    System.out.println(dateFormat.format(date) + " 15秒内总共收到的条数：" + events.size());
                    /**
                    for(JSONObject event:events){
                        System.out.println(event);
                    }
                    **/
                    out.collect(events);
                }
            }
        });


        result.print("已窗口批量转化数据");

        env.execute("rocketmq-connect-flink");
    }
}