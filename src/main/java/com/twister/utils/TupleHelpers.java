package com.twister.utils;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

public final class TupleHelpers {

    private TupleHelpers() {
    }

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
    
    public static List<String> unwrap(List<List> allValues, int index) {
        List<String> values = new ArrayList<String>();
        for (List allValue : allValues) {
            String line = (String) allValue.get(index);
            values.add(line);
        }
        return values;
    }

}
