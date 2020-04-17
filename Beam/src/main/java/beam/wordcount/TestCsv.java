package beam.wordcount;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import beam.wordcount.Test.BatchOptions;

public class TestCsv {
	public interface BatchOptions extends PipelineOptions {

	    @Description("Path to the data file(s) containing game data.")
	    
	    @Default.String("D:\\beam\\test1.csv")
	    String getInput();
	    void setInput(String value);

	   
	}
	static class CsvParser extends DoFn <ReadableFile, CSVRecord>{
		@DoFn.ProcessElement
		public void processElement(@Element ReadableFile element, DoFn.OutputReceiver<CSVRecord> receiver) throws IOException {
		    
		InputStream is = Channels.newInputStream(element.open());
		 Reader reader = new InputStreamReader(is);
		 Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader("Month,Expense").withDelimiter(',').withFirstRecordAsHeader().parse(reader);
		    
		    for (CSVRecord record : records) { 
		    	String s= record.get(1);
		    	String x= record.get(0);
		    	System.out.println(x+" "+s);
		    	receiver.output(record);
	}}}
	public static void main(String[] args) {
		BatchOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BatchOptions.class);
	    Pipeline pipeline = Pipeline.create(options);
		
		
		PCollection lines=pipeline
				.apply(FileIO.match().filepattern(options.getInput()))
				.apply(FileIO.readMatches())
				.apply(ParDo.of(new CsvParser()));
		pipeline.run().waitUntilFinish();

	}

}
