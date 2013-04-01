package com.twister.utils;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

public class FileUtils {
	
	public static void writeFile(String filename, String text) {
		RandomAccessFile rf;
		try {
			rf = new RandomAccessFile(filename, "rw");
			rf.seek(rf.length()); // 将指针移动到文件末尾
			String toCn = new String(text.getBytes("UTF-8"), "8859_1");
			rf.writeBytes(toCn + "\n");
			rf.close(); // 关闭文件流
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}
	
}
