package com.gm.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.Watermark;

public class EventWaterMark implements WatermarkGeneratorSupplier<JSONObject> {
    private static final long serialVersionUID = -2338922000184097299L;
    private long currentMaxTimestamp;
    private String eventTimeFieldName;
    private long maxOutOfOrderness;

    // 当前数据进入的时间
    private long currentDateTimeMillis;

    public EventWaterMark(Window window) {
        this.maxOutOfOrderness = window.getMaxOutOfOrderness();
        this.eventTimeFieldName = window.getAttrField();
    }

    @Override
    public WatermarkGenerator<JSONObject> createWatermarkGenerator(Context context) {
        return new WatermarkGenerator<JSONObject>() {

            @Override
            public void onEvent(JSONObject jsonObject, long l, WatermarkOutput watermarkOutput) {
                currentMaxTimestamp = Math.max(currentMaxTimestamp, jsonObject.getLong(eventTimeFieldName));

                // 记录 当前来的数据来到 时间戳
                currentDateTimeMillis = System.currentTimeMillis();
            }

            /**
             * 此方法会根据 配置的 setAutoWatermarkInterval 周期定时执行
             * @param watermarkOutput
             */
            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                // 无后续数据，最后一个窗口不会关闭计算，
                // watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness));

                // 使用System.currentTimeMillis()，如果第一条数据时间很早， 窗口不会进行关闭计算
                // watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1L));

                // 无初始数据进入 watermark不用推进， 多并行度时候，会取最小的watermark，依然不会触发窗口，针对数据源的并行度可以设置为1解决此问题。
                if (currentMaxTimestamp - maxOutOfOrderness <= 0) {
                } else {
                    // （系统当前时间 - 上次事件到达的系统时间） >= 10，，watermark保持向后推进， 确保无数据进入，窗口能闭窗计算
                    if (System.currentTimeMillis() - currentDateTimeMillis >= 10000L) {
                        // 无数据进入，保持watermark递增，当前水位线 = 最后事件时间 - 乱序时间 + 递增时间 - 1L
                        currentMaxTimestamp = currentMaxTimestamp - maxOutOfOrderness + 10000L - 1L;
                        watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp));

                        // 模拟数据进入 设置数据进入时间
                        currentDateTimeMillis = System.currentTimeMillis();
                    } else {
                        watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1L));
                    }
                }
            }
        };
    }
}
