package crawler;

import java.io.FileInputStream;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import crawler.resource.manager.DBManager;
import crawler.resource.manager.FileManager;
import crawler.util.CrawlerUtil;

public class Crawler {
	public static Set<String> visited;
	public static String startUrl;
	public static String year;
	public String configFile;
	DBManager dbManager;
	FileManager fileManager;
	public int sequenceNumber = 0;
	public static Map<Integer, String> urlsCacheMap;

	public Crawler() throws Exception {
		try {
			configFile = "crawler.conf";
			readConfigFile(configFile);
			urlsCacheMap = new HashMap<Integer, String>();
			fileManager = new FileManager("urls.txt");
			dbManager = new DBManager(configFile);
			if (dbManager != null) {
				System.out.println("truncating the table");
				dbManager.truncateTable();
			}
		} catch (SQLException sq) {
			System.out.println("DB Connection failed");
		} catch (Exception e) {
			System.out.println("Exception while creating connections"
					+ e.toString());
		}
	}

	// Read Configuration file crawler.conf
	private void readConfigFile(String fileName) throws Exception {
		Properties configFile = new Properties();
		configFile.load(new FileInputStream(fileName));
		startUrl = configFile.getProperty("url").trim();
		year = configFile.getProperty("year").trim();
		System.out.println("url:" + startUrl + " year:" + year);
	}

	public static void main(String[] args) throws Exception {
		Crawler crawler = new Crawler();
		System.out.println("Crawling starting for the given url :" + startUrl);
		crawler.processPage(startUrl);
		System.out.println("Starting dumping into OUTPUT Resource");
		crawler.dumpCacheInDB(urlsCacheMap);
	}

	// process url
	public void processPage(String URL) throws Exception {
		// System.out.println(URL);
		if (visited == null) {
			visited = new HashSet<String>();
		}
		if (CrawlerUtil.verifyUrl(URL)) {
			// System.out.println(URL);
			urlsCacheMap.put(++sequenceNumber, URL);
		}
		try {
			// Using the Jsoup framework to parse the urls
			Document doc = null;
			// trying minimum 2 times to get connection if timeout occurs
			for (int i = 1; i <= 2; i++) {
				try {
					doc = Jsoup.connect(URL).get();
					break; // Break immediately if successful
				} catch (SocketTimeoutException e) {
					System.out.println("Timeout exception " + e.toString());
				}
			}
			// get all links and recursively call the processPage method
			Elements questions = doc.select("a[href]");
			for (Element link : questions) {
				String urlString = link.attr("abs:href");
				if (!visited.contains(urlString) && urlString.contains(year)) {
					visited.add(urlString);
					System.out.println(urlString);

					if (!urlString.contains(CrawlerConstants.HASH_ARCHIVES)
							&& (CrawlerUtil.matchPattern(urlString, year))) {
						processPage(urlString);
					}
				}
			}
		} catch (Exception e) {
			System.out
					.println("Not able to parse the given url because of firewall issue "
							+ e.toString());
		}
	}

	// dump the collected mails urls in db in one shot
	public void dumpCacheInDB(Map<Integer, String> urlsCacheMap)
			throws Exception {
		if (urlsCacheMap != null && urlsCacheMap.size() > 0) {
			System.out.println("number of emails : " + urlsCacheMap.size());
			if (fileManager != null) {
				System.out.println("Dumping Data into a file");
				fileManager.storeToFile(urlsCacheMap);
			}
			if (dbManager != null) {
				System.out.println("Dumping Data into DB");
				dbManager.storeToDB(urlsCacheMap);
			}
		}
	}
}