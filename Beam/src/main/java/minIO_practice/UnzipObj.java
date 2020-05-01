package minIO_practice;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

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
import org.apache.commons.compress.utils.IOUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;



public class UnzipObj {
	
	 public static class unzip extends DoFn<FileIO.ReadableFile, String> {
		 @DoFn.ProcessElement
		 public void processElement(@Element ReadableFile element, DoFn.OutputReceiver<String> receiver) throws IOException {
			 System.out.println("Inside function ");
			 InputStream is = Channels.newInputStream(element.open());
			 TarArchiveInputStream tis = new TarArchiveInputStream(is);
//			 File destFile = new File("s3://radiantlabs/sanfran-sfd0413c/results/simulations_untar/un_zip");
			 File destFile = new File("C:\\Users\\vpst1\\Downloads\\Compressed\\unzip");
	            
	            if(!destFile.exists()){
	                 destFile.mkdir();
	                 System.out.println("directory made");
	            }
	            TarArchiveEntry tarEntry = null;
             System.out.println("starting to unzip");
	            while ((tarEntry = tis.getNextTarEntry()) != null) {
	                File outputFile = new File(destFile + File.separator + tarEntry.getName());
                System.out.println(" File ---- " + outputFile.getPath());
	                if(tarEntry.isDirectory()){
//	                	System.out.println("outputFile Directory ---- " 
//	                            + outputFile.getAbsolutePath());
	                    if(!outputFile.exists()){
	                        outputFile.mkdirs();
	                    }
	                }else{
                	  System.out.println("outputFile File ---- " + outputFile.getPath()+"\n--------"+outputFile.getParent());
	                    outputFile.getParentFile().mkdirs();
	                    FileOutputStream fos = new FileOutputStream(outputFile); 
	                    IOUtils.copy(tis, fos);
	                    fos.close();
	                }   
	            }
	            tis.close();
	 }
	 }
	
	public interface CustomerOfflineDataOptions extends PipelineOptions {
		@Default.String("s3://radiantlabs/sanfran-sfd0413c/results/simulation_output/simulations_job0.tar.gz")
		String getInputFile();

		void setInputFile(String file);
	}

	public static void main(String[] args) {
		CustomerOfflineDataOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(CustomerOfflineDataOptions.class);
		Pipeline pipeline = Pipeline.create(options);
		AWSCredentials awsCredentials = new BasicAWSCredentials("cred1", "cred2");
		options.as(AwsOptions.class).setAwsCredentialsProvider(new AWSStaticCredentialsProvider(awsCredentials));
		options.as(AwsOptions.class).setAwsRegion("US-WEST-2");
//		System.out.println(options);
		PCollection input = pipeline.apply("ReadInput", FileIO.match().filepattern(options.getInputFile()))
				.apply(FileIO.readMatches().withCompression(Compression.GZIP));
		input.apply(ParDo.of(new unzip()));

		pipeline.run().waitUntilFinish();
	}

}
