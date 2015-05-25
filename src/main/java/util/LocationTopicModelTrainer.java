package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import jgibblda.LDA3;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import ckling.text.Text;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

/**
 * 
 * @author Martin Koerner <info@mkoerner.de>
 *
 */
public class LocationTopicModelTrainer {

	public static void main(String[] args) {
		LocationTopicModelTrainer locationTopicModelTrainer = new LocationTopicModelTrainer();
		// locationTopicModelTrainer.createTrainingFile(
		// "/home/martin/reveal/files/snow14_testset_tweets_small.zip",
		// "/home/martin/reveal/files/training/snow14.txt", true);
		locationTopicModelTrainer.setTrainingFile(new File(
				"/home/martin/reveal/files/training/snow14.txt"));
		locationTopicModelTrainer.trainTopicModel();
	}

	private File trainingFile = null;

	public LocationTopicModelTrainer() {
		// for language detection
		try {
			String dirname = "profiles/";
			Enumeration<URL> en = Detector.class.getClassLoader().getResources(
					dirname);
			List<String> profiles = new ArrayList<>();
			if (en.hasMoreElements()) {
				URL url = en.nextElement();
				JarURLConnection urlcon = (JarURLConnection) url
						.openConnection();
				try (JarFile jar = urlcon.getJarFile();) {
					Enumeration<JarEntry> entries = jar.entries();
					while (entries.hasMoreElements()) {
						String entry = entries.nextElement().getName();
						if (entry.startsWith(dirname)) {
							try (InputStream in = Detector.class
									.getClassLoader()
									.getResourceAsStream(entry);) {
								profiles.add(IOUtils.toString(in));
							}
						}
					}
				}
			}

			DetectorFactory.loadProfile(profiles);
		} catch (IOException | LangDetectException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void createTrainingFile(String inputFilePath, String ouputFilePath,
			boolean stemTweetText) {
		try {
			// open zip file that contains json files containing
			ZipFile zipFile = new ZipFile(inputFilePath);
			Enumeration<? extends ZipEntry> entries = zipFile.entries();

			// open output file
			String tmpOutputFilePath = ouputFilePath + "_tmp";
			BufferedWriter bufferedTmpWriter = new BufferedWriter(
					new FileWriter(tmpOutputFilePath));
			long numberOfTweets = 0;

			Text text = new Text();
			text.setLang("en");
			// iterate over json files in zip file
			while (entries.hasMoreElements()) {
				ZipEntry zipEntry = entries.nextElement();
				System.out.println("processing: " + zipEntry.getName());
				// open reader for current json file
				BufferedReader bufferedReader = new BufferedReader(
						new InputStreamReader(zipFile.getInputStream(zipEntry)));

				String line;
				while ((line = bufferedReader.readLine()) != null) {
					// create json object from current line (tweet)
					JSONObject obj = new JSONObject(line);
					if (!obj.get("coordinates").toString().equals("null")) {
						// get coordinates (W,N)
						JSONArray coordinates = obj
								.getJSONObject("coordinates").getJSONArray(
										"coordinates");

						String tweetText;
						if (stemTweetText) {
							tweetText = "";
							text.setText(obj.get("text").toString());
							Iterator<String> textIterator = text.getTerms();
							while (textIterator.hasNext()) {
								if (!tweetText.isEmpty()) {
									tweetText += " ";
								}
								tweetText += textIterator.next();
							}
						} else {
							tweetText = obj.get("text").toString();

						}

						try {
							Detector detector = DetectorFactory.create();
							detector.append(tweetText);
							String langDetected;
							langDetected = detector.detect();
							if (langDetected.equals("en")) {
								bufferedTmpWriter.write(coordinates.get(0)
										+ " " + coordinates.get(1) + " "
										+ tweetText + "\n");
								numberOfTweets++;
							}
						} catch (LangDetectException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					}
				}
				bufferedReader.close();
			}
			bufferedTmpWriter.close();
			zipFile.close();

			this.trainingFile = new File(ouputFilePath);
			// add number of tweets to file
			BufferedReader bufferedTmpReader = new BufferedReader(
					new FileReader(tmpOutputFilePath));
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(
					trainingFile));
			bufferedWriter.write(numberOfTweets + "\n");
			String tmpLine;
			while ((tmpLine = bufferedTmpReader.readLine()) != null) {
				bufferedWriter.write(tmpLine += "\n");
			}
			bufferedTmpReader.close();
			bufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void trainTopicModel() {
		if (this.trainingFile == null) {
			throw new IllegalAccessError(
					"specify training File or run createTrainingFile()");
		}
		// run topic model trainer
		String dir = "/home/martin/reveal/files/training/";
		String name = "snow.txt";
		String arg = "-dir " + dir + " -dfile " + name + ".txt " + "-est "
				+ "-L 849 " + "-beta 0.1 " + "-betaa 1 " + "-betab 1 "
				+ "-gamma 1.0 " + "-gammaa 1.0 " + "-gammab 0.1 "
				+ "-delta 10.0 " + "-Alpha 1 " + "-Alphaa 1 " + "-Alphab 1 "
				+ "-alpha0 1 " + "-alpha0a 1 " + "-alpha0b 1 " + "-savestep 5 "
				+ "-twords 20 " + "-niters 2 " + "-runs 1 "
				+ "-sampleHyper true";
		String[] args = arg.split(" ");
		LDA3.main(args);
	}

	public File getTrainingFile() {
		return trainingFile;
	}

	public void setTrainingFile(File trainingFile) {
		this.trainingFile = trainingFile;
	}
}
