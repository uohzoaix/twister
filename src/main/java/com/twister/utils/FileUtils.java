package com.twister.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

/**
 * Utility class to manipulate file.
 */
public class FileUtils {
	
	private FileUtils() {
	}
	
	public static File createTempFile() throws IOException {
		File file = File.createTempFile("twister", ".tmp");
		System.out.print(file.getName() + " : " + file.getAbsolutePath());
		file.deleteOnExit();
		return file;
	}
	
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
	 * Util methods to write a defined number of random alphanumeric string (50
	 * characters) in a file.
	 * <p/>
	 * 
	 * @param file
	 *            the file to write
	 * @param numberOfLines
	 *            the number of line to write in the file
	 * @return the list of lines written in the file
	 * @throws IOException
	 */
	public static List<String> writeRandomLines(File file, int numberOfLines) throws IOException {
		return writeRandomLines(file, numberOfLines, 0, TimeUnit.MILLISECONDS);
	}
	
	/**
	 * Util methods to write a defined number of random alphanumeric string (50
	 * characters) in a file.
	 * <p/>
	 * A delay may be set to write a new line after a time period.
	 * 
	 * @param file
	 *            the file to write
	 * @param numberOfLines
	 *            the number of line to write in the file
	 * @param wait
	 *            delay to write a new line in file
	 * @param timeUnit
	 *            the time unit
	 * @return the list of lines written in the file
	 * @throws IOException
	 */
	public static List<String> writeRandomLines(File file, int numberOfLines, long wait, TimeUnit timeUnit)
			throws IOException {
		FileWriter fw = new FileWriter(file);
		List<String> lines = new ArrayList<String>(numberOfLines);
		try {
			for (int i = 1; i <= numberOfLines; i++) {
				// String line = "Line " + i ;
				String line = randomAlphanumeric(50);
				lines.add(line);
				fw.append(line + "\n");
				fw.flush();
				TimeUtils.sleep(wait, timeUnit);
			}
		} finally {
			IOUtils.closeQuietly(fw);
		}
		return lines;
	}
}