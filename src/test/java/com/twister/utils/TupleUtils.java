package com.twister.utils;

import java.util.ArrayList;
import java.util.List;

public class TupleUtils {

    private TupleUtils() {
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