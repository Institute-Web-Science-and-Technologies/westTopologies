package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class HTMLMapFilterer {

	public static void main(String[] args) {

		HTMLMapFilterer htmlMapFilterer = new HTMLMapFilterer();
		// htmlMapFilterer
		// .filterMap(
		// "/home/martin/reveal/files/training/model-final.map3.html",
		// "/home/martin/reveal/files/training/model-final.map3.0_99.html",
		// 0.99);
		// htmlMapFilterer
		// .filterMap(
		// "/home/martin/reveal/files/training/model-final.map3.html3.html",
		// "/home/martin/reveal/files/training/model-final.map3.html3.0_9.html",
		// 0.9);
		//
		htmlMapFilterer
				.filterMap(
						"/home/martin/reveal/files/training/model-final.map3.htmlgrid.htm",
						"/home/martin/reveal/files/training/model-final.map3.htmlgrid.0_95.htm",
						0.95);
	}

	public void filterMap(String inputFilePath, String outputFilePath,
			double filterProbability) {
		File inputFile = new File(inputFilePath);
		File outputFile = new File(outputFilePath);

		try {
			BufferedReader inputReader = new BufferedReader(new FileReader(
					inputFile));
			BufferedWriter outputWriter = new BufferedWriter(new FileWriter(
					outputFile));

			String line = "";
			String lineBuffer = "";
			while ((line = inputReader.readLine()) != null) {
				if (lineBuffer.isEmpty()) {
					if (line.startsWith("var")) {
						lineBuffer += line += "\n";
					} else {
						outputWriter.write(line + "\n");
					}
				} else {
					// lineBuffer is not empty
					if (line.isEmpty()) {
						if (Math.random() > filterProbability) {
							outputWriter.write(lineBuffer);
						}
						lineBuffer = "";
					} else {
						lineBuffer += line += "\n";
					}
				}
			}
			inputReader.close();
			outputWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
