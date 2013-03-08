package com.twister.nio.log;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.twister.utils.Common;

public class AccessLogStatics {
	/**
	 * 统计类
	 * 
	 * @author zhouguoqing
	 * 
	 */
	
	private Fields _fields = new Fields("cnt_pv", "cnt_bytes", "cnt_time", "avg_time", "max_time", "min_time",
			"cnt_error", "a", "b", "c", "d", "e");
	
	private static Map<String, Values> _result = new ConcurrentHashMap<String, Values>();
	private static long rs_cnt = 0l;
	
	public AccessLogStatics() {
	}
	
	public AccessLogStatics(List<String> fields) {
		this._fields = new Fields(fields);
	}
	
	public AccessLogStatics(String... fields) {
		this._fields = new Fields(fields);
	}
	
	public synchronized void calculate(String key, int response_code, long content_length, long request_time) {
		int[] art = Common.assess_request_time(response_code, request_time);
		
		// init
		Values vals = new Values(0l, 0l, 0l, 0.0, Long.MIN_VALUE, Long.MAX_VALUE, 0, 0, 0, 0, 0, 0);
		if (_result.containsKey(key)) {
			vals = _result.get(key);
		}
		
		long cnt_pv = (long) vals.get(_fields.fieldIndex("cnt_pv")) + 1l;
		long cnt_bytes = (long) vals.get(_fields.fieldIndex("cnt_bytes")) + content_length;
		long cnt_time = (long) vals.get(_fields.fieldIndex("cnt_time")) + request_time;
		vals.set(_fields.fieldIndex("cnt_pv"), cnt_pv);
		vals.set(_fields.fieldIndex("cnt_bytes"), cnt_bytes);
		vals.set(_fields.fieldIndex("cnt_time"), cnt_time);
		// other cnt
		BigDecimal bd = new BigDecimal(cnt_time);
		double avg_time = cnt_pv == 0 ? 0 : bd.divide(BigDecimal.valueOf(cnt_pv), 2, BigDecimal.ROUND_FLOOR)
				.doubleValue();
		
		vals.set(_fields.fieldIndex("avg_time"), avg_time);
		long max_time = (long) vals.get(_fields.fieldIndex("max_time"));
		if (request_time > 0 && request_time > max_time)
			max_time = request_time;
		vals.set(_fields.fieldIndex("max_time"), max_time);
		
		long min_time = (long) vals.get(_fields.fieldIndex("min_time"));
		if (request_time > 0 && request_time < min_time)
			min_time = request_time;
		vals.set(_fields.fieldIndex("min_time"), min_time);
		
		vals.set(_fields.fieldIndex("cnt_error"), (int) vals.get(_fields.fieldIndex("cnt_error")) + art[0]);
		vals.set(_fields.fieldIndex("a"), (int) vals.get(_fields.fieldIndex("a")) + art[1]);
		vals.set(_fields.fieldIndex("b"), (int) vals.get(_fields.fieldIndex("b")) + art[2]);
		vals.set(_fields.fieldIndex("c"), (int) vals.get(_fields.fieldIndex("c")) + art[3]);
		vals.set(_fields.fieldIndex("d"), (int) vals.get(_fields.fieldIndex("d")) + art[4]);
		vals.set(_fields.fieldIndex("e"), (int) vals.get(_fields.fieldIndex("e")) + art[5]);
		_result.put(key, vals);
		rs_cnt += 1;
		
	}
	
	public Fields get_fields() {
		return _fields;
	}
	
	public void set_fields(Fields _fields) {
		this._fields = _fields;
	}
	
	public Map<String, Values> get_result() {
		return _result;
	}
	
	public Values get_Values(String key) {
		if (get_result().containsKey(key)) {
			return get_result().get(key);
		} else {
			return new Values();
		}
	}
	
	@Override
	public String toString() {
		String str = "rs_cnt: " + rs_cnt + " ,ConcurrentHashMap: " + get_result().size() + "\n"
				+ get_fields().toString() + "\n" + get_result().toString();
		return str;
	}
	
	public static void main(String vs[]) {
		// AccessLogStatics als = new AccessLogStatics();
		// als.calculate("aaa", 200, 4, 15);
		// als.calculate("aaa", 408, 1, 100);
		// als.calculate("aaa", 200, 105, 80);
		// System.out.println(als.toString());
	}
	
}