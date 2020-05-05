package minIO_practice;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

import minIO_practice.UnzipObj.CustomerOfflineDataOptions;
import minIO_practice.UnzipObj.unzip;

public class UnzipData {

 static	AmazonS3 s3client;
	 public static class unzip extends DoFn<FileIO.ReadableFile, String> {
		 String des;
		 Map<String, String> meta = new HashMap<String, String>();
		 ObjectMetadata medata = new ObjectMetadata();
		 unzip(String des){
			 this.des=des;
			 
			 Date date = new Date();  
	            SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");  
	            String strDate = formatter.format(date);  
	            meta.put("date", strDate);
	            medata.setUserMetadata(meta);

		 }
		 @DoFn.ProcessElement
		 public void processElement(@Element ReadableFile element, DoFn.OutputReceiver<String> receiver) throws IOException {
			 System.out.println("Inside function ");
			 try {
			 InputStream is = Channels.newInputStream(element.open());
			 TarArchiveInputStream tis = new TarArchiveInputStream(is);
			 TarArchiveEntry tarEntry = tis.getNextTarEntry();

				
		        while (tarEntry != null) {
		        	if(!exists(s3client,des+tarEntry.getName()))
					 {
		            byte[] btoRead = new byte[1024];
		            ByteArrayOutputStream bout = new ByteArrayOutputStream();
		            int len = 0;
		            while ((len = tis.read(btoRead)) != -1) {
		                bout.write(btoRead, 0, len);
		            }
		            bout.close();
		            byte[] b=bout.toByteArray(); 
		            ByteArrayInputStream bais =new ByteArrayInputStream(b);
		           
		           
		            System.out.println("untar--"+tarEntry.getName());
		            s3client.putObject("radiantlabs", des+tarEntry.getName(),bais,medata);
		       		
		       		bais.close();
					 }
		        	else {
		        		for(int i=0;i<=4;i++)
		        		{
		        			tarEntry = tis.getNextTarEntry();
		        		}
		        	}
		            tarEntry = tis.getNextTarEntry();
		        
			 }
		        System.out.println("done");
		        tis.close();
		      		tis.close();
		      
		    } catch (Exception e) {
		      System.out.println("Error occurred: " + e);
		    }
			 
	 }
	 }
	 	static void init(String accessKey, String secretKey) {
	 		 s3client = AmazonS3ClientBuilder
					  .standard()
					  .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey,secretKey)))
					  .withRegion(Regions.US_WEST_2)
					  .build();
	 		 System.out.println("aws client created");
			
		}
	 
		
		  static boolean exists(AmazonS3 s3,String path) {
			    try {
			    //	System.out.println("exist ="+path);
			        s3.getObjectMetadata("radiantlabs", path); 
			    } catch(AmazonServiceException e) {
			    //	System.out.println("no key");
			        return false;
			    }
			  //  System.out.println("key present");
			    return true;
			}
	 
	 
	
	public interface CustomerOfflineDataOptions extends PipelineOptions {
		@Default.String("s3://radiantlabs/sanfran-sfd0413c/results/simulation_output/simulations_job1001.tar.gz")
		String getInputFile();
		void setInputFile(String file);
		String getOutputFile();
		void setOutputFile(String des);
		String getAccessKey();
		void setAccessKey(String cred1);
		String getSecretKey();
		void setSecretKey(String cred2);
		
	}

	public static void main(String[] args) {
		
		CustomerOfflineDataOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(CustomerOfflineDataOptions.class);
		
		AWSCredentials awsCredentials = new BasicAWSCredentials(options.getAccessKey(),options.getSecretKey());
		options.as(AwsOptions.class).setAwsCredentialsProvider(new AWSStaticCredentialsProvider(awsCredentials));
		options.as(AwsOptions.class).setAwsRegion("us-west-2");
		Pipeline pipeline = Pipeline.create(options);
		PCollection input = pipeline.apply("ReadInput", FileIO.match().filepattern(options.getInputFile()))
				.apply(FileIO.readMatches().withCompression(Compression.GZIP));
		init(options.getAccessKey(), options.getSecretKey());
		
		input.apply(ParDo.of(new unzip(options.getOutputFile())));

		pipeline.run().waitUntilFinish();
	}

	

}
