package Practice_Basic;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


/*data in csv file
cat	3
dog	3
horse	5
elephant	8
lion	4
mouse	5

 */

public class ReadCSV {

	public static void main(String[] args) {
		PipelineOptions options= PipelineOptionsFactory.create();
		Pipeline pipeline=Pipeline.create(options);
		PCollection<String> lines=pipeline.apply("Read from file",TextIO.read().from("D:\\beam\\test1.csv"));

		
		PCollection words =lines.apply(MapElements.via(new SimpleFunction<String, List<String>>(){
			@Override
			public List<String> apply(String input){
				return Arrays.asList(input.split(","));
			}
			}));
		PCollection<String> word =(PCollection<String>) words.apply(Flatten.iterables());
		
		PCollection<Integer> wordLength = (PCollection<Integer>) word.apply(ParDo.of(new DoFn<String, Integer>() {
			@ProcessElement
			public void processElement(@Element String word, OutputReceiver<Integer> out) {
				if(word.length()>1) {
				out.output(word.length());
			}}
		}));

		wordLength.apply(MapElements.via(new SimpleFunction<Integer,String>(){
			@Override
			public String apply(Integer input) {
				return String.format("%d",input);
			}
		})).apply(TextIO.write().to("D:\\beam\\wordlength12").withSuffix(".txt"));
		pipeline.run().waitUntilFinish();
	}

}
