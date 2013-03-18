package com.twister.spout.trans;

import java.io.Serializable;
import java.math.BigInteger;

public class TransactionMetadata implements Serializable{	
	private static final long serialVersionUID = 1L;
	int index;
	int amt;
	
	// for kryo compatibility
	public TransactionMetadata() {
		
	}
	
	public TransactionMetadata(int index, int amt) {
		this.index = index;
		this.amt = amt;
	}
	
	@Override
	public String toString() {
		return "index: " + index + "; amt: " + amt;
	}
	
}
