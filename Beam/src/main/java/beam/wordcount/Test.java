package beam.wordcount;


import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class Test {
	
	public interface BatchOptions extends PipelineOptions {

	    @Description("Path to the data file(s) containing game data.")
	    
	    @Default.String("D:\\beam\\test1.csv")
	    String getInput();
	    void setInput(String value);

	   
	}


		  static class FilterCSVHeaderFn extends DoFn<String, KV<String,Integer>> {
		    String headerFilter;

		    public FilterCSVHeaderFn(String headerFilter) {
		      this.headerFilter = headerFilter;
		    }

		    @ProcessElement
		    public void processElement(ProcessContext c) {
		      String row = c.element();
		      
		      if (!row.equals(this.headerFilter)) {
		    	  String[] data=row.split(",");
		  
		        c.output(KV.of(data[0],Integer.parseInt(data[1])));
		      }
		    }
		  }

	public static void main(String[] args) {
		//PipelineOptions options= PipelineOptionsFactory.create();
		//Pipeline pipeline=Pipeline.create(options);
		BatchOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BatchOptions.class);
	    Pipeline pipeline = Pipeline.create(options);
		
		
		PCollection<String> lines=pipeline
		        .apply(TextIO.read().from(options.getInput()));
		
		String header = "Month,Expense";
		PCollection<KV<String,Integer>>rows= lines.apply(ParDo.of(new FilterCSVHeaderFn(header)));

		PCollection<KV<String, Integer>> yearlyExpense = (PCollection<KV<String, Integer>>) rows
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
		yearlyExpense.apply(MapElements.via(new SimpleFunction<KV<String, Integer>,String>(){
			@Override
			public String apply(KV<String, Integer> input) {
				return String.format("%s,%s",input.getKey(), input.getValue());
			}
		}))
		.apply(TextIO.write().to("D:\\beam\\csv\\csv_output").withHeader("Month,Expense").withSuffix(".csv").withoutSharding());
		pipeline.run().waitUntilFinish();
		

	}

}
