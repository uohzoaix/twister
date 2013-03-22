package com.twister;

public class Tobj {
	static class A {
		public String n = "aaaa";
		public int pv = 0;
		public int err = 0;
		
		A() {
		}
		
		public int getPv() {
			return pv;
		}
		
		public void setPv(int pv) {
			this.pv = pv;
		}
		
		public int getErr() {
			return err;
		}
		
		public void setErr(int err) {
			this.err = err;
		}
		
		public void add(A o) {
			this.pv += o.pv;
			this.err += o.err;
		}
		
		@Override
		public String toString() {
			return "A [pv=" + pv + ", err=" + err + "]";
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		A obj = new A();
		for (int i = 0; i < 20; i++) {
			A obj1 = new A();
			obj1.setPv(1);
			obj1.setErr(1);
			obj.add(obj1);
			System.out.println(i + " " + obj.toString());
			
		}
		
	}
	
}
