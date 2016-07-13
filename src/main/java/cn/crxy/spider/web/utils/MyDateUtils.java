package cn.crxy.spider.web.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class MyDateUtils {
	public static String dateFormat2String = "yyyy-MM-dd HH:mm:ss";

	public static SimpleDateFormat dateFormat1 = new SimpleDateFormat(
			"yyyy��MM��dd�� HHʱmm�� E");
	public static SimpleDateFormat dateFormat2 = new SimpleDateFormat(
			MyDateUtils.dateFormat2String);
	public static SimpleDateFormat dateFormat3 = new SimpleDateFormat(
			"yyyy��MM��dd��  E");
	public static SimpleDateFormat dateFormat4 = new SimpleDateFormat(
			"yyyy-MM-dd");
	public static SimpleDateFormat dateFormat5 = new SimpleDateFormat(
			"MM��dd��HHʱ E");
	public static SimpleDateFormat dateFormat6 = new SimpleDateFormat("yyyy-MM");
	public static SimpleDateFormat dateFormat7 = new SimpleDateFormat(
			"yyyy��MM��dd�� HHʱmm��ss��");

	/**
	 * ��ʽ��yyyy��MM��dd�� HHʱmm�� E
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate1(Date date) {
		return dateFormat1.format(date);
	}

	/**
	 * ��ʽ��yyyy-MM-dd HH:mm:ss
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate2(Date date) {
		return dateFormat2.format(date);
	}

	/**
	 * ��ʽ��yyyy��MM��dd�� E
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate3(Date date) {
		return dateFormat3.format(date);
	}

	/**
	 * ��ʽ��yyyy-MM-dd
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate4(Date date) {
		return dateFormat4.format(date);
	}

	/**
	 * ��ʽ��MM��dd��HHʱ E
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate5(Date date) {
		return dateFormat5.format(date);
	}

	/**
	 * ��ʽ��yyyy-MM
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate6(Date date) {
		return dateFormat6.format(date);
	}

	/**
	 * ��ʽ��yyyy��MM��dd��HHʱmm��ss��
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate7(Date date) {
		return dateFormat7.format(date);
	}

	public static Date today() {
		Calendar cal = new GregorianCalendar();// Ĭ���ǵ�ǰʱ��(long)
		return cal.getTime();
	}

	public static Date yesterday() {
		Calendar cal = new GregorianCalendar();// Ĭ���ǵ�ǰʱ��(long)
		cal.add(Calendar.DAY_OF_MONTH, -1);
		return cal.getTime();
	}

	public static Date monthBeginDate() {
		Calendar cal = new GregorianCalendar();// Ĭ���ǵ�ǰʱ��(long)
		cal.set(Calendar.DAY_OF_MONTH, 1);
		return cal.getTime();
	}

	public static Date monthEndDate() {
		Calendar cal = new GregorianCalendar();// Ĭ���ǵ�ǰʱ��(long)
		cal.set(Calendar.DAY_OF_MONTH, 0);
		return cal.getTime();
	}

	/**
	 * �Զ�����ǰ����
	 * 
	 * @param formatString
	 *            ��ʽ
	 * @param dateString
	 *            ����ֵ
	 * @return
	 */
	public static Date format(String formatString, String dateString) {
		try {
			return (new SimpleDateFormat(formatString)).parse(dateString);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * ��ʽ������yyyy-MM-dd
	 * 
	 * @param dateString
	 * @return
	 */
	public static Date format(String dateString) {
		try {
			return dateFormat4.parse(dateString);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * ��ȡ����ʱ��Ĳ������ʽ��XX��XXСʱXX��XX��
	 * 
	 * @param time1
	 * @param time2
	 * @return
	 */
	public static String getTimeDiff(long time1, long time2) {
		long l = time2 - time1;
		long day = l / (24 * 60 * 60 * 1000);
		long hour = (l / (60 * 60 * 1000) - day * 24);
		long min = ((l / (60 * 1000)) - day * 24 * 60 - hour * 60);
		long s = (l / 1000 - day * 24 * 60 * 60 - hour * 60 * 60 - min * 60);
		return "" + day + "��" + hour + "Сʱ" + min + "��" + s + "��";
	}

	public static void main(String[] args) {
		Calendar cal = new GregorianCalendar();// Ĭ���ǵ�ǰʱ��(long)
		cal.add(Calendar.MINUTE, 30);
		System.out.println(MyDateUtils.formatDate7(cal.getTime()));
	}
}
