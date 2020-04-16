package Practice_Basic;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class CsvRead {
	public interface Options extends PipelineOptions {
	    @Description("Input file path")
	    @Validation.Required
	    String getInputFile();
	    void setInputFile(String value);

	    @Description("Output directory")
	    @Validation.Required
	    String getOutputDir();
	    void setOutputDir(String value);
	}
	static class CsvParser extends DoFn<ReadableFile,CSVRecord> {
		@DoFn.ProcessElement
		public void processElement(@Element ReadableFile element, DoFn.OutputReceiver<CSVRecord> receiver) throws IOException {
		    InputStream is = Channels.newInputStream(element.open());
		    Reader reader = new InputStreamReader(is);
		    Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader("Month","Expense").withDelimiter(',').withFirstRecordAsHeader().parse(reader);
		    
		    for (CSVRecord record : records) { receiver.output(record); }
		}
	}

	public static void main(String[] args) {
		
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class); 
		Pipeline p = Pipeline.create(options);
		
		PCollection<CSVRecord>records= p.apply(FileIO.match().filepattern(options.getInputFile())) 
        .apply(FileIO.readMatches()) 
        .apply(ParDo.of(new CsvParser())) ;
	
		
		p.run().waitUntilFinish();
	}

}
