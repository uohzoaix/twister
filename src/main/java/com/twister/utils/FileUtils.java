package com.twister.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

/**
 * Utility class to manipulate file.
 */
public class FileUtils {
	private static Logger logger = LoggerFactory.getLogger(FileUtils.class);
	
	public FileUtils() {
	}

	public static String createDataStr() {
		Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		String dateString = formatter.format(currentTime);
		return dateString;
	}
	
	public static String getAccessFile() {
		return "logs/AccessLog_" + createDataStr() + ".log";
	}

	public static void dumperValue(final String filename, final String line) {
		String tmp = line;
		if (tmp.endsWith("\n")) {
			tmp += "\n";
		}
		FileWriter fw = null;
		try {
			fw = new FileWriter(filename, true);
			fw.write(tmp);
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static File createTempFile() {
		File file = null;
		try {
			file = File.createTempFile("twister-", ".tmp");
			logger.debug(file.getName() + " : " + file.getAbsolutePath());
			file.deleteOnExit();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return file;
	}
	
	public static void writeFile(String filename, String text, boolean isappend) {
		RandomAccessFile rf;
		try {
			rf = new RandomAccessFile(filename, "rw");
			if (isappend) {
				rf.seek(rf.length()); // 将指针移动到文件末尾
			}
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
	
	public static ArrayList<String> listFile(String filename) {
		File file = new File(filename);
		ArrayList<String> ls = new ArrayList<String>();
		if (!file.exists()) {
			try {
				file.createNewFile();
				ls.add(file.toString());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		File dir = new File(file.getParent());
		if (dir.isDirectory()) {
			String[] fs = dir.list();
			for (String fileName : fs) {
				File f = new File(dir.getPath() + File.separator + fileName);
				if (f.isFile()) {
					ls.add(f.toString());
				} else if (f.isDirectory()) {
					ls.add(f.toString());
				}
			}
		}
		return ls;
	}

	public static void fileCopy(String filein, String fileout) {
		try {
			FileInputStream fi = new FileInputStream(filein);
			FileOutputStream fo = new FileOutputStream(fileout);
			byte[] buff = new byte[256];
			int len = 0;
			while ((len = fi.read(buff)) > 0) {
				fo.write(buff, 0, len);
			}
			fi.close();
			fo.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static Logger debug(String text) {
		logger.debug(text);
		return logger;
	}
	

}
