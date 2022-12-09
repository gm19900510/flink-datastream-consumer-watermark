package com.gm.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.eventtime.Watermark;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WatermarkFunction implements WatermarkStrategy<JSONObject> {

    @Override
    public WatermarkGenerator<JSONObject> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<JSONObject>() {
            final private Tuple2<Long, Boolean> state = Tuple2.of(0L, true);

            final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            private long maxWatermark;

            // 每一个数据处理时 都会调用该方法，在这里用于更新最大时间戳，记录你处理过的数据中最大的时间戳
            @Override
            public void onEvent(JSONObject waterSensor, long l, WatermarkOutput watermarkOutput) {
                maxWatermark = Math.max(maxWatermark, waterSensor.getLong("eventTime"));
                state.f0 = System.currentTimeMillis();
                state.f1 = false;
            }

            // 周期行调用的方法 默认为200ms，该方就是生成watermark的方法，窗口最大时间小于等于当前的水印时间，则触发计算。
            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                // 乱序时间5秒
                long outOfTime = 5000L;
                // 无初始数据进入
                if (maxWatermark - outOfTime <= 0) {
                    // System.out.println("无操作");
                } else {

                    //（系统当前时间 - 上次事件到达的系统时间） >= 10，表示10秒内无数据进入，watermark保持向后推进，确保无数据进入，窗口能闭窗计算；state.f1 = true，表示进入周期性调用。
                    if (System.currentTimeMillis() - state.f0 >= 10000L && !state.f1) {
                        // 无数据进入，watermark + 递增时间 - outOfTime - 1：当前水位线 = 最后事件时间 + 递增时间 - 乱序时间 - 1L
                        watermarkOutput.emitWatermark(new Watermark(maxWatermark + 10000L - outOfTime - 1L));
                        // 进入周期性调用
                        state.f1 = true;
                        // 重新设置maxWatermark，设置数据进入时间
                        maxWatermark = System.currentTimeMillis();
                    } else {
                        // maxWatermark - outOfTime - 1L 周期性调度生成watermark方法
                        watermarkOutput.emitWatermark(new Watermark(maxWatermark - outOfTime - 1L));
                    }
                }
            }
        };
    }
}
