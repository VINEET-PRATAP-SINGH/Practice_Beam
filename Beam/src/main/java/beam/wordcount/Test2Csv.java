package beam.wordcount;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import beam.wordcount.TestCsv.BatchOptions;
import beam.wordcount.TestCsv.CsvParser;

public class Test2Csv {
	public interface BatchOptions extends PipelineOptions {

		@Description("Path to the data file(s) containing game data.")

		@Default.String("D:\\beam\\test1.csv")
		String getInput();

		void setInput(String value);

	}

	static class CsvParser extends DoFn<ReadableFile, KV<String, Integer>> {
		@DoFn.ProcessElement
		public void processElement(@Element ReadableFile element, DoFn.OutputReceiver<KV<String, Integer>> receiver)
				throws IOException {

			InputStream is = Channels.newInputStream(element.open());
			
			Reader reader = new InputStreamReader(is);
			Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader("Month,Expense").withDelimiter(',')
					.withFirstRecordAsHeader().parse(reader);

			for (CSVRecord record : records) {
				//System.out.println(record.get(0) + " " + record.get(1));
				receiver.output(KV.of(record.get(0), Integer.parseInt(record.get(1))));
			}
		}
	}

	public static void main(String[] args) {
		BatchOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BatchOptions.class);
		Pipeline pipeline = Pipeline.create(options);
		
		File dir = new File(options.getInput());
		for (File file : dir.listFiles()) {
		
			String inputString = file.toString();
			PCollection lines = pipeline.apply("Match Files", FileIO.match().filepattern(inputString))
					.apply("Read Files", FileIO.readMatches())
					.apply(ParDo.of(new CsvParser()));
			PCollection<KV<String, Integer>> yearlyExpense = (PCollection<KV<String, Integer>>) lines
					.apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
						@ProcessElement
						public void processElement(ProcessContext p) {// to check which month has high expense than 300
																		// and then store in pcollection
							KV<String, Integer> value = p.element();
							if (value.getValue() > 300) {
								p.output(KV.of(value.getKey(), value.getValue()));
							}

						}
					}));
			yearlyExpense.apply(MapElements.via(new SimpleFunction<KV<String, Integer>, String>() {
				@Override
				public String apply(KV<String, Integer> input) {
					return String.format("%s,%s", input.getKey(), input.getValue());
				}
			})).apply(TextIO.write().to("D:\\beam\\o_p").withHeader("Month,Expense").withSuffix(".csv")
					.withoutSharding());
			
			pipeline.run().waitUntilFinish();

		}
	}

}
