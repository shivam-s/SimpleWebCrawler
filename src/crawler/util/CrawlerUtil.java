package crawler.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import crawler.CrawlerConstants;

public class CrawlerUtil {
	public static String pattern1 = CrawlerConstants.MAIL_BOX
			+ CrawlerConstants.SLASH_PECENTIAL;
	public static String pattern2 = CrawlerConstants.MAIL_BOX
			+ CrawlerConstants.SLASH_THREAD;

	// filtering the urls those are to be stored
	public static boolean verifyUrl(String URL) {
		if (URL.contains(CrawlerConstants.AT_THE_RATE)
				&& !URL.contains(CrawlerConstants.HASH_ARCHIVES)
				&& !URL.contains(CrawlerConstants.SLASH_THREAD)
				&& !URL.contains(CrawlerConstants.SLASH_BROWSER)
				&& !URL.contains(CrawlerConstants.SLASH_DATE)
				&& !URL.contains(CrawlerConstants.SLASH_AUTHOR)) {
			return true;
		}
		return false;
	}

	// pattern matcher
	public static boolean matchPattern(String line, String year) {
		// String pattern = year + "12.*mbox[/][^r]";

		Pattern r1 = Pattern.compile(year + pattern1);
		Pattern r2 = Pattern.compile(year + pattern2);
		Matcher m1 = r1.matcher(line);
		Matcher m2 = r2.matcher(line);
		if (m1.find() || m2.find()) {
			return true;
		} else {
			return false;
		}
	}
}
