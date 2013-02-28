package com.twister.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import static java.lang.Integer.valueOf;
import static java.lang.String.valueOf;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

public final class TupleUtils {

    private TupleUtils() {
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
    
    public static final void StringToList(String message, List<String> list) {
		if(message == null) {
			return;
		}
		synchronized (list) {
			list.clear();
			String[] domains = message.split(",");
			if(domains != null) {
				list.addAll(Arrays.asList(domains));
			}
		}
	}
    

	/**
	 * Same as s[:3], s[3:], s[-1], etc. <b>WARN</b> This method does not
	 * validate if the indexes at <pre>i</pre> parameter are really numbers.
	 * 
	 * @param target
	 * @param i
	 * @return
	 */
	public static String substring(final String target, final String i) {
		final String[] is = i.split(":", 2);
		final int len = target.length();
		if (!i.contains(":"))
			return valueOf(target.charAt(norm(len, valueOf(is[0]))));
		else if (i.startsWith(":"))
			return target.substring(0, norm(len, valueOf(is[1])));
		else if (i.endsWith(":"))
			return target.substring(norm(len, valueOf(is[0])));
		else
			return target.substring(norm(len, valueOf(is[0])),
					norm(len, valueOf(is[1])));
	}

	/**
	 * Return a new index 'normalized', that is, for negative index, points to
	 * the corresponding index at the end.
	 * 
	 * @param len
	 * @param i
	 * @return
	 */
	private static int norm(final int len, final int i) {
		return i < 0 ? len + i : i;
	}

}
