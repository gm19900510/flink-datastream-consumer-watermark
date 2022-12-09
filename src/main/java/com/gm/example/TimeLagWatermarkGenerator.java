package com.gm.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class TimeLagWatermarkGenerator  implements AssignerWithPeriodicWatermarks<JSONObject> {

    private final long maxTimeLag = 5000L; // 5 seconds

    @Override
    public long extractTimestamp(JSONObject element, long previousElementTimestamp) {
        return element.getLong("eventTime");
    }

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }
}