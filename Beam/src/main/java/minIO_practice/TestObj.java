package minIO_practice;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;

import beam.wordcount.TestCsv.BatchOptions;
import io.minio.MinioClient;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;

public class TestObj {
/*
	public interface BatchOptions extends PipelineOptions {
		@Default.String("s3://minio-001/*.tar.gz")
		
		String getInput();
	    void setgetInput(String file);
	    
	   
	}
*/	
	public static void main(String[] args) throws InvalidEndpointException, InvalidPortException {
		MinioClient minioClient = new MinioClient("https://play.min.io","Q3AM3UQ867SPQQA43P2F","zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG");
		 PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline=Pipeline.create(options);
		
		System.out.println(options);
  		PCollection files=pipeline.apply(FileIO.match().filepattern("s3://minio-001/simulations_job0.tar.gz"))
				.apply(FileIO.readMatches().withCompression(Compression.GZIP));
  		pipeline.run().waitUntilFinish();

	}

}
