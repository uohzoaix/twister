package com.twister.nio.log;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import com.twister.utils.Common;
 
 
/**
 * pojo 
 * @author guoqing
 *
 */
@SuppressWarnings("serial")
public class AccessLogAnalysis extends AbstractAnalysis<AccessLogAnalysis> implements Serializable {
	
	// jiekou,转成long分，抛弃秒值
	// ukey=time#rely#server#uriname
	// 20120613#10:01:00#0#/home
	private String key;
	private long cnt_pv = 1l;
	private long cnt_bytes = 0l; // 流量kb
	private long cnt_time = 0l; // 响应ms
	private double avg_time = 0l;
	private int code=200;
	private long cnt_error = 0l;
	// requesttime 优秀 良好 达标 超时 异常
	private long a = 0l;
	private long b = 0l;
	private long c = 0l;
	private long d = 0l;
	private long e = 0l;
	 
	public AccessLogAnalysis(){}
	/**
	 * default
	 * @param ukey
	 * @param day
	 * @param response_code
	 * @param content_length
	 * @param request_time
	 */
	public AccessLogAnalysis(String ukey,int response_code, long content_length, long request_time){
		// cnt_error 优秀 良好 达标 超时 异常		
		this.key=ukey;		 
		this.cnt_pv = 1l;
		this.code=response_code;
		this.cnt_bytes = content_length;
		this.cnt_time = request_time;
		this.setAvg_time(request_time);
		this.assess_request_time(response_code, request_time);
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
	
	public void setAvg_time(double cnt_time) {
		BigDecimal bd = new BigDecimal(cnt_time);
		this.avg_time = this.cnt_pv == 0 ? 0 : bd.divide(BigDecimal.valueOf(this.cnt_pv), 2, BigDecimal.ROUND_FLOOR).doubleValue();		 
	}
	 
	
	public int getCode() {
		return code;
	}
	public void setCode(int code) {
		this.code = code;
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
	
	@Override
	public void assess_request_time(int response_code, long request_time) {
		int[] art = Common.assess_request_time(response_code, request_time);	 
		this.cnt_error = art[0];
		this.a = art[1];
		this.b = art[2];
		this.c = art[3];
		this.d = art[4];
		this.e = art[5];		
	}
	 
	@Override
	public String getKey() {		 
		return key;
	}
	@Override
	public void setKey(String ukey) {
		this.key=ukey;		
	}
 
 
	
	@Override
	public AccessLogAnalysis calculate(AccessLogAnalysis obj) {
		if (this.key.equals(obj.key)){
			this.cnt_pv += obj.cnt_pv;
			this.cnt_bytes +=obj.cnt_bytes;
			this.cnt_time +=obj.cnt_time;
			this.code=obj.code;
			this.setAvg_time(this.cnt_time);
			this.cnt_error +=obj.cnt_error;
			this.a+=obj.a;
			this.b+=obj.b;
			this.c+=obj.c;
			this.d+=obj.d;
			this.e+=obj.e;			
			return this;
		}else{
			return obj;
		}
	}
	
	@Override
	public String toString() {
		return "AccessLogAnalysis [key=" + key + ", cnt_pv=" + cnt_pv + ", cnt_bytes=" + cnt_bytes + ", cnt_time="
				+ cnt_time + ", avg_time=" + avg_time + ", code=" + code + ", cnt_error=" + cnt_error + ", a=" + a
				+ ", b=" + b + ", c=" + c + ", d=" + d + ", e=" + e + "]";
	}   
	
	public void addObject(Map<String, AccessLogAnalysis> bag, String key, AccessLogAnalysis objValue) {
		AccessLogAnalysis slot = bag.get(key);
		if (slot == null) {
			bag.put(key, slot);
		} else {
			slot = slot.calculate(objValue);
		}
		bag.put(key, slot);
	}
	
	public void addCnt(Map<String, Integer> bag, String key) {
		Integer ct = bag.get(key);
		if (ct == null) {
			bag.put(key, 1);
		} else {
			ct += 1;
			bag.put(key, 1);
		}
	}
	
}
