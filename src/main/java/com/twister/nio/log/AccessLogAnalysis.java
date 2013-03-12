package com.twister.nio.log;

import java.io.Serializable;

import com.twister.utils.Common;
 
/**
 * pojo 
 * @author guoqing
 *
 */
@SuppressWarnings("serial")
public class AccessLogAnalysis implements Serializable {
	
	// jiekou,转成long分，抛弃秒值
	// key=datestr_yyyymmdd hh:mm:ss
	// ukey=time|method|uriname|code|rely|server|getProv
	private String ukey;
	private String day = "19700101";
	private long cnt_pv = 1l;
	private long cnt_bytes = 0l; // 流量kb
	private long cnt_time = 0l; // 响应ms
	private double avg_time = 0l;
	private long max_time = 0l;
	private long min_time = 0l;
	private long cnt_error = 0l;
	// requesttime 优秀 良好 达标 超时 异常
	private long a = 0l;
	private long b = 0l;
	private long c = 0l;
	private long d = 0l;
	private long e = 0l;
	 
	public AccessLogAnalysis(){
		
	}
	/**
	 * default
	 * @param ukey
	 * @param day
	 * @param response_code
	 * @param content_length
	 * @param request_time
	 */
	public AccessLogAnalysis(String ukey,String day,int response_code, long content_length, long request_time){
		// cnt_error 优秀 良好 达标 超时 异常
		int[] art = Common.assess_request_time(response_code, request_time);
		this.ukey=ukey;
		this.day=day;
		this.cnt_pv = 1l;
		this.cnt_bytes = content_length;
		this.cnt_time = request_time;
		this.avg_time = request_time;
		this.max_time = request_time;
		this.min_time = request_time;
		this.cnt_error = art[0];
		this.a = art[1];
		this.b = art[2];
		this.c = art[3];
		this.d = art[4];
		this.e = art[5];
	}
	
	/**
	 * auto constructor
	 * @param ukey
	 * @param day
	 * @param cnt_pv
	 * @param cnt_bytes
	 * @param cnt_time
	 * @param avg_time
	 * @param max_time
	 * @param min_time
	 * @param cnt_error
	 * @param a
	 * @param b
	 * @param c
	 * @param d
	 * @param e
	 *  
	 */
	public AccessLogAnalysis(String ukey, String day, long cnt_pv, long cnt_bytes, long cnt_time, double avg_time,
			long max_time, long min_time, long cnt_error, long a, long b, long c, long d, long e) {
		super();
		this.ukey = ukey;
		this.day = day;
		this.cnt_pv = cnt_pv;
		this.cnt_bytes = cnt_bytes;
		this.cnt_time = cnt_time;
		this.avg_time = avg_time;
		this.max_time = max_time;
		this.min_time = min_time;
		this.cnt_error = cnt_error;
		this.a = a;
		this.b = b;
		this.c = c;
		this.d = d;
		this.e = e;
	}

	public String getUkey() {
		return ukey;
	}
	
	public void setUkey(String ukey) {
		this.ukey = ukey;
	}
	
	public long getCnt_pv() {
		return cnt_pv;
	}
	
	public void setCnt_pv(long cnt_pv) {
		this.cnt_pv = cnt_pv;
	}
	
	public long getCnt_bytes() {
		return cnt_bytes;
	}
	
	public void setCnt_bytes(long cnt_bytes) {
		this.cnt_bytes = cnt_bytes;
	}
	
	public long getCnt_time() {
		return cnt_time;
	}
	
	public void setCnt_time(long cnt_time) {
		this.cnt_time = cnt_time;
	}
	
	public double getAvg_time() {
		return avg_time;
	}
	
	public void setAvg_time(double avg_time) {
		this.avg_time = avg_time;
	}
	
	public long getMax_time() {
		return max_time;
	}
	
	public void setMax_time(long max_time) {
		this.max_time = max_time;
	}
	
	public long getMin_time() {
		return min_time;
	}
	
	public void setMin_time(long min_time) {
		this.min_time = min_time;
	}
	
	public long getCnt_error() {
		return cnt_error;
	}
	
	public void setCnt_error(long cnt_error) {
		this.cnt_error = cnt_error;
	}
	
	public long getA() {
		return a;
	}
	
	public void setA(long a) {
		this.a = a;
	}
	
	public long getB() {
		return b;
	}
	
	public void setB(long b) {
		this.b = b;
	}
	
	public long getC() {
		return c;
	}
	
	public void setC(long c) {
		this.c = c;
	}
	
	public long getD() {
		return d;
	}
	
	public void setD(long d) {
		this.d = d;
	}
	
	public long getE() {
		return e;
	}
	
	public void setE(long e) {
		this.e = e;
	}
	
	public String getDay() {
		return day;
	}
	
	public void setDay(String day) {
		this.day = day;
	}
	
}
