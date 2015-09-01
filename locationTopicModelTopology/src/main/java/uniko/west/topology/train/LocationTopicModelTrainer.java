package uniko.west.topology.train;

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
import java.util.zip.ZipException;
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
		File trainingFile = null;

		File twitterZipFile = null;
		String[] skipKeywords = new String[0];
		switch (args.length) {
		case 1:
			trainingFile = new File(args[0]);
			System.out.println("Reading training file: "
					+ trainingFile.getAbsolutePath());
			locationTopicModelTrainer.setTrainingFile(trainingFile);
			locationTopicModelTrainer.trainTopicModel();
			return;
		case 2:
			trainingFile = new File(args[0]);
			twitterZipFile = new File(args[1]);
			break;
		case 3:
			trainingFile = new File(args[0]);
			twitterZipFile = new File(args[1]);

			skipKeywords = args[2].split(",");
			break;
		default:
			throw new IllegalArgumentException(
					"provide arguments for creating or loading a trainingFile from which the topic model is built:\n"
							+ "\targuments for building a training file while \n"
							+ "\t\tskipping files in zip file containing keywords: <path-to-training-file> <path-to-twitter-zip> <keyword1>,<keywords2>...\n"
							+ "\targuments for building a training file: <path-to-training-file> <path-to-twitter-zip>\n"
							+ "\targuments for building a training file: <path-to-training-file>\n"
							+ "\tin all cases, the topic model is built in the same folder as the training file");
		}
		System.out.println("Building training file: "
				+ trainingFile.getAbsolutePath() + "\n"
				+ "Using zipped twitter data from: "
				+ twitterZipFile.getAbsolutePath());
		locationTopicModelTrainer.createTrainingFile(twitterZipFile,
				trainingFile, true, true, skipKeywords);

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

	public void createTrainingFile(File twitterZipFile, File trainingFile,
			boolean stemTweetText, boolean removeSingleCharacterWords,
			String[] skipKeywords) {
		// open zip file that contains json files containing
		ZipFile zipFile;
		try {
			zipFile = new ZipFile(twitterZipFile);
			Enumeration<? extends ZipEntry> entries = zipFile.entries();

			// open temorary output file
			// the temorary file is needed because we have to add the number of
			// tweets to the first line of the training file later on

			File tmpOutputFile = new File(trainingFile.getAbsolutePath()
					+ "_tmp");
			BufferedWriter bufferedTmpWriter = new BufferedWriter(
					new FileWriter(tmpOutputFile));
			long numberOfTweets = 0;

			Text text = new Text();
			text.setLang("en");
			// iterate over json files in zip file
			while (entries.hasMoreElements()) {
				ZipEntry zipEntry = entries.nextElement();

				boolean skipEntry = false;
				// skip entries that contain skipKeywords
				for (String skipKeyword : skipKeywords) {
					if (zipEntry.getName().contains(skipKeyword)) {
						skipEntry = true;
					}
				}
				if (skipEntry) {
					System.out.println("skipping: " + zipEntry.getName());
					continue;
				}

				System.out.println("processing: " + zipEntry.getName());
				// open reader for current json file
				BufferedReader bufferedReader = new BufferedReader(
						new InputStreamReader(zipFile.getInputStream(zipEntry)));

				String line;
				while ((line = bufferedReader.readLine()) != null) {
					// create json object from current line (tweet)
					JSONObject jsonObject = null;
					try {
						jsonObject = new JSONObject(line);
					} catch (org.json.JSONException jsonException) {
						// skip files with broken JSON
						break;
					}

					if (!jsonObject.get("coordinates").toString()
							.equals("null")) {
						// get coordinates (W,N)
						JSONArray coordinates = jsonObject.getJSONObject(
								"coordinates").getJSONArray("coordinates");

						String tweetText;
						if (stemTweetText) {
							tweetText = "";
							text.setText(jsonObject.get("text").toString());
							Iterator<String> textIterator = text.getTerms();
							while (textIterator.hasNext()) {
								if (!tweetText.isEmpty()) {
									tweetText += " ";
								}
								tweetText += textIterator.next();
							}
						} else {
							tweetText = jsonObject.get("text").toString();
						}
						if (removeSingleCharacterWords) {
							// remove words that consist of one character (like
							// & , . 1 2 3 ...)
							String[] tweetTextSplit = tweetText.split("\\s");
							tweetText = "";
							for (String tweetWord : tweetTextSplit) {
								if (tweetText.length() > 0) {
									tweetText += " ";
								}
								if (tweetWord.length() > 1) {
									tweetText += tweetWord;
								}
							}
							tweetText = tweetText.replaceAll("\\s\\s+", " ");
						}

						try {
							Detector detector = DetectorFactory.create();
							detector.append(tweetText);
							String langDetected;
							langDetected = detector.detect();
							if (langDetected.equals("en")) {
								double longitude = coordinates.getDouble(0);
								double latitude = coordinates.getDouble(1);
								if (longitude != 0.0 && latitude != 0.0) {
									bufferedTmpWriter.write(latitude + " "
											+ longitude + " " + tweetText
											+ "\n");
									numberOfTweets++;
								}
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

			this.trainingFile = trainingFile;
			// add number of tweets to file
			BufferedReader bufferedTmpReader = new BufferedReader(
					new FileReader(tmpOutputFile));
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(
					this.trainingFile));
			bufferedWriter.write(numberOfTweets + "\n");
			String tmpLine;
			while ((tmpLine = bufferedTmpReader.readLine()) != null) {
				bufferedWriter.write(tmpLine += "\n");
			}
			bufferedTmpReader.close();
			// delete temporary output file
			tmpOutputFile.delete();
			bufferedWriter.close();
		} catch (ZipException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void trainTopicModel() {
		if (this.trainingFile == null) {
			throw new IllegalAccessError(
					"specify training File (setTrainingFile()) or run createTrainingFile()");
		}
		// run topic model trainer
		// TODO: cleanup args
		String dir = this.trainingFile.getParent();
		String name = this.trainingFile.getName();
		String arg = "-dir " + dir + " -dfile " + name + " -est " + "-L 849 "
				+ "-beta 0.1 " + "-betaa 1 " + "-betab 1 " + "-gamma 1.0 "
				+ "-gammaa 1.0 " + "-gammab 0.1 " + "-delta 10.0 "
				+ "-Alpha 1 " + "-Alphaa 1 " + "-Alphab 1 " + "-alpha0 1 "
				+ "-alpha0a 1 " + "-alpha0b 1 " + "-savestep 5 "
				+ "-twords 20 " + "-niters 200 " + "-sampleHyper true";
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
