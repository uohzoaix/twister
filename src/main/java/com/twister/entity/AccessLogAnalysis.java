package com.twister.entity;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.twister.utils.Common;

/**
 * pojo
 * 
 * @author guoqing
 * 
 */
@SuppressWarnings("serial")
public class AccessLogAnalysis extends AbstractAnalysis<AccessLogAnalysis> implements Serializable, Cloneable {

	// jiekou,转成long分，抛弃秒值
	// ukey=time#rely#server#uriname
	// 20120613#10:01:00#0#/home
	private String ukey;
	private long cnt_pv = 1l;
	private long cnt_bytes = 0l; // 流量kb
	private long cnt_time = 0l; // 响应ms
	private double avg_time = 0l;
	private int code = 200;
	private long cnt_error = 0l;
	// requesttime 优秀 良好 达标 超时 异常
	private long a = 0l;
	private long b = 0l;
	private long c = 0l;
	private long d = 0l;
	private long e = 0l;
	private String txid = "0";

	public AccessLogAnalysis() {
	}

	/**
	 * default
	 * 
	 * @param ukey
	 * @param day
	 * @param response_code
	 * @param content_length
	 * @param request_time
	 */
	public AccessLogAnalysis(String ukey, int response_code, long content_length, long request_time) {
		// cnt_error 优秀 良好 达标 超时 异常
		this.ukey = ukey;
		this.cnt_pv = 1l;
		this.code = response_code;
		this.cnt_bytes = content_length;
		this.cnt_time = request_time;
		this.Avgtime(request_time);
		this.assess_request_time(response_code, request_time);
	}

	/**
	 * 
	 * @param newobj
	 * @param oldobj
	 * @return this
	 */
	@Override
	public void calculate(AccessLogAnalysis obj) {
		this.cnt_pv += obj.cnt_pv;
		this.cnt_bytes += obj.cnt_bytes;
		this.cnt_time += obj.cnt_time;
		this.code = obj.code;
		this.Avgtime(this.cnt_time);
		this.cnt_error += obj.cnt_error;
		this.a += obj.a;
		this.b += obj.b;
		this.c += obj.c;
		this.d += obj.d;
		this.e += obj.e;
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

	public void setAvg_time(double avgtime) {
		this.avg_time = avgtime;
	}

	public void Avgtime(long cnt_time) {
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

	// taskid or TransactionAttemptid
	public String getTxid() {
		return txid;
	}

	public void setTxid(String txid) {
		this.txid = txid;
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
	public String toString() {
		return "AccessLogAnalysis [ukey=" + ukey + ", cnt_pv=" + cnt_pv + ", cnt_bytes=" + cnt_bytes + ", cnt_time=" + cnt_time + ", avg_time=" + avg_time
				+ ", txid=" + txid + "]";
	}

	public String repr() {
		return "AccessLogAnalysis [ukey=" + ukey + ", cnt_pv=" + cnt_pv + ", cnt_bytes=" + cnt_bytes + ", cnt_time=" + cnt_time + ", avg_time=" + avg_time
				+ ", code=" + code + ", cnt_error=" + cnt_error + ", a=" + a + ", b=" + b + ", c=" + c + ", d=" + d + ", e=" + e + "txid=" + txid + "]";
	}

	@Override
	public Object clone() {
		Object o = null;
		try {
			o = (AccessLogAnalysis) super.clone(); // Object
													// 中的clone()识别出你要复制的是哪一个对象。
		} catch (Exception e) {
			e.printStackTrace();
		}
		return o;
	}

	public Map<String, String> splitUkey(int logver, String uk, String SEPARATOR) {
		Map<String, String> mp = new HashMap<String, String>();
		String[] ks = uk.split(SEPARATOR);
		try {
			if (logver == 1) {
				// ver=1
				// 0#20130401#11:23:00#1#01#/home
				if (ks.length > 0)
					mp.put("logver", ks[0]);
				if (ks.length > 1)
					mp.put("day", ks[1]);
				if (ks.length > 2)
					mp.put("createat", ks[2]);
				if (ks.length > 3)
					mp.put("rely", ks[3]);
				if (ks.length > 4)
					mp.put("server", ks[4]);
				if (ks.length > 5)
					mp.put("uriname", ks[5]);

			} else {
				// 0#20130401#11:23:00#1#01
				mp.put("logver", ks[0]);
				mp.put("day", ks[1]);
				mp.put("createat", ks[2]);
				mp.put("rely", ks[3]);
				mp.put("server", ks[4]);

			}
			return mp;
		} catch (Exception e) {
			e.printStackTrace();

		}
		return mp;
	}

	public Map<String, String> toAllMap() {
		Map<String, String> mp1 = splitUkey(0, ukey, "#");
		Map<String, String> mp2 = toMap();
		mp1.putAll(mp2);
		return mp1;
	}

	public Map<String, String> toMap() {
		Map<String, String> mp = new HashMap<String, String>();
		mp.put("ukey", ukey);
		mp.put("cnt_pv", String.valueOf(cnt_pv));
		mp.put("cnt_bytes", String.valueOf(cnt_bytes));
		mp.put("cnt_time", String.valueOf(cnt_time));
		mp.put("avg_time", String.valueOf(avg_time));
		mp.put("code", String.valueOf(code));
		mp.put("cnt_error", String.valueOf(cnt_error));
		mp.put("a", String.valueOf(a));
		mp.put("b", String.valueOf(b));
		mp.put("c", String.valueOf(c));
		mp.put("d", String.valueOf(d));
		mp.put("e", String.valueOf(e));
		mp.put("txid", String.valueOf(txid));
		return mp;
	}

	public BasicDBObject toBasicDBObject() {
		Map<String, String> mp = this.toAllMap();
		BasicDBObject dbobj = new BasicDBObject(mp);
		return dbobj;
	}

}
