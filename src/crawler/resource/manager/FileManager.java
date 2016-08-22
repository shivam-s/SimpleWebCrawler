package crawler.resource.manager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class FileManager {
	public FileWriter fileWriter;
	public BufferedWriter bufferedWriter;

	public FileManager(String fileName) throws IOException {
		fileWriter = new FileWriter(fileName);
		bufferedWriter = new BufferedWriter(fileWriter);
	}

	public void storeToFile(Map<Integer, String> urlsCacheMap)
			throws IOException {
		int sequenceNumber = 0;
		String URL;
		Iterator<Integer> iterator = urlsCacheMap.keySet().iterator();
		try {
			while (iterator.hasNext()) {
				sequenceNumber = (int) iterator.next();
				URL = urlsCacheMap.get(sequenceNumber);

				if (fileWriter == null) {
					fileWriter = new FileWriter("urls.txt");
				}
				if (bufferedWriter == null) {
					bufferedWriter = new BufferedWriter(fileWriter);
				}
				bufferedWriter.write(URL);
				bufferedWriter.newLine();
			}
			System.out
					.println("Data has been stored into the file successfully");
		} catch (Exception ex) {
			System.out
					.println("Exception while connecting and inserting into the file"
							+ ex.toString());
		} finally {
			System.out.println("closing connections");
			if (bufferedWriter != null) {
				bufferedWriter.close();
			}
			if (fileWriter != null) {
				fileWriter.close();
			}
		}
	}
}
