package Practice_Basic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/*
 * csv file content
month       expense
jan			200
feb			231
mar			300
apr			390
may			250
jun			290
jul			321
aug			330
sep			259
oct			378
nov			231
dec			399

*what this code will do it will select months which have expense greater than 300 and write it to local storage as month => expense
*if multiple files passed than it will select only that month which has high expense <if both file have same month than only>
 */
public class MultiCsvRead {

	public static void main(String[] args) {

		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options); // create pipeline

		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		Map<String, Integer> expenses = new HashMap<String, Integer>();

		System.out.println("press 1 to read a single file or press 2 to read multiple files"); // ask user to process
																								// one file or more
		Scanner sc = new Scanner(System.in);
		int choice = sc.nextInt();
		if (choice == 1) { // process only one file
			System.out.println("enter the path"); // get file path from user like D:/beam/csv/2020.csv
			String csvFile = sc.next();
			try {

				br = new BufferedReader(new FileReader(csvFile));
				while ((line = br.readLine()) != null) {

					String[] fileContent = line.split(cvsSplitBy);

					expenses.put(fileContent[0], Integer.parseInt(fileContent[1])); // saves record to list

				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("enter the folder path"); // to read multiple files
			String srcDir = sc.next();// to read folder path from user which contains all csv files like  D:/beam/csv/

			File folder = new File(srcDir); 
										
			File[] listOfFiles = folder.listFiles();
			try {

				if (listOfFiles.length > 0) {	//to check if folder has any file
					for (int i = 0; i < listOfFiles.length; i++) {//will iterate all files in folder
						br = new BufferedReader(new FileReader(srcDir + listOfFiles[i].getName()));
						while ((line = br.readLine()) != null) {

							String[] fileContent = line.split(cvsSplitBy);
							if (expenses.containsKey(fileContent[0])) {		//check if the month from current file is stored or not
								if (expenses.get(fileContent[0]) < Integer.parseInt(fileContent[1])) {// if stored will check which one has high expense
									expenses.put(fileContent[0], Integer.parseInt(fileContent[1]));	//store expense if current year has high expense
								}
							} else {
								expenses.put(fileContent[0], Integer.parseInt(fileContent[1]));// stores record
							}
						}
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		PCollection<KV<String, Integer>> montlyExpense = pipeline.apply(Create.of(expenses));//will create a pcollection of list

		PCollection<KV<String, Integer>> yearlyExpense = (PCollection<KV<String, Integer>>) montlyExpense
				.apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
					@ProcessElement
					// public void processElement(@Element Integer value, OutputReceiver<Integer>
					// out) {
					public void processElement(ProcessContext p) {// to check which month has high expense than 300 and then store in pcollection
						KV<String, Integer> value = p.element();
						if (value.getValue() > 300) {
							p.output(KV.of(value.getKey(), value.getValue()));
						}

					}
				}));
		yearlyExpense.apply(MapElements.via(new SimpleFunction<KV<String, Integer>, String>() {// it will write the pcollection in to local storage
			@Override
			public String apply(KV<String, Integer> input) {
				return String.format("%s => %s", input.getKey(), input.getValue());
			}
		})).apply(TextIO.write().to("D:\\beam\\csv\\yearly_expense").withSuffix(".txt"));

		pipeline.run().waitUntilFinish();

	}

}
