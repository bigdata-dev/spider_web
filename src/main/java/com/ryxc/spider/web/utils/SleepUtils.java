package com.ryxc.spider.web.utils;

public class SleepUtils {
	public static void sleep(long millin){
		try {
			Thread.currentThread().sleep(millin);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
