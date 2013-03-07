package com.twister.io.output;

import java.math.BigDecimal;
import java.math.BigInteger;

public class AccessLogStatics {
	
	/**
	 * 统计类，用于统计数据的，最大、最小、sum，计数，均值，方差等。
	 * 
	 */
	
	private final String delemer = ",";
	private long max = Long.MIN_VALUE;
	private long min = Long.MAX_VALUE;
	private long count = 0l;
	private BigInteger sum = BigInteger.ZERO;
	private BigInteger sumSqua = BigInteger.ZERO;
	
	public AccessLogStatics() {
	}
	
	/**
	 * feed一个数值
	 * 
	 * @param val
	 */
	public void feed(long val) {
		if (val > max)
			max = val;
		if (val < min)
			min = val;
		count++;
		sum = sum.add(BigInteger.valueOf(val));
		sumSqua = sumSqua.add(BigInteger.valueOf(val).pow(2));
	}
	
	/**
	 * max value
	 * 
	 * @return
	 */
	public long getMax() {
		return this.max;
	}
	
	/**
	 * min value
	 * 
	 * @return
	 */
	public long getMin() {
		return this.min;
	}
	
	/**
	 * count
	 * 
	 * @return
	 */
	public long getCount() {
		return this.count;
	}
	
	/**
	 * sum of all the feedin values
	 * 
	 * @return
	 */
	public long getSum() {
		return this.sum.longValue();
	}
	
	/**
	 * the average
	 * 
	 * @param scale
	 *            scale of the BigDecimal quotient to be returned.均值
	 * @return
	 */
	public double getAvg(int scale) {
		BigDecimal bd = new BigDecimal(sum);
		return this.count == 0 ? 0 : bd.divide(BigDecimal.valueOf(this.count), scale, BigDecimal.ROUND_FLOOR)
				.doubleValue();
	}
	
	/**
	 * the stander
	 * 
	 * @param scale
	 *            scale of the BigDecimal quotient to be returned 方差
	 * @return
	 */
	public double getStdDev(int scale) {
		BigDecimal bd = new BigDecimal(this.sumSqua);
		bd = bd.divide(BigDecimal.valueOf(this.count), scale, BigDecimal.ROUND_FLOOR);
		bd = bd.subtract(BigDecimal.valueOf(getAvg(scale)).pow(2));
		
		return this.count == 0 ? 0 : round(Math.sqrt(bd.doubleValue()), scale);
	}
	
	/**
	 * 提供精确的小数位四舍五入处理。
	 * 
	 * @param v
	 *            需要四舍五入的数字
	 * @param scale
	 *            小数点后保留几位
	 * @return 四舍五入后的结果
	 */
	public static double round(double v, int scale) {
		if (scale < 0) {
			throw new IllegalArgumentException("The scale must be a positive integer or zero");
		}
		BigDecimal b = new BigDecimal(Double.toString(v));
		BigDecimal one = new BigDecimal("1");
		return b.divide(one, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
	
	public String toString(String delemer, int scale) {
		StringBuffer sb = new StringBuffer();
		sb.append("min:").append(getMin()).append(delemer);
		sb.append("max:").append(getMax()).append(delemer);
		sb.append("count:").append(getCount()).append(delemer);
		sb.append("sum:").append(getSum()).append(delemer);
		sb.append("avg:").append(getAvg(scale)).append(delemer);
		sb.append("dev:").append(getStdDev(scale));
		return sb.toString();
	}
	
	@Override
	public String toString() {
		return toString(delemer, 4);
	}
	
	public static void main(String vs[]) {
	}
}