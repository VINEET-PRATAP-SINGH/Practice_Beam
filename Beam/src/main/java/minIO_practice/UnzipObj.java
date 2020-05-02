package minIO_practice;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
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
import org.apache.commons.compress.utils.IOUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import io.minio.PutObjectOptions;
import io.minio.errors.MinioException;



public class UnzipObj {
	static AWSCredentials awsCredentials = new BasicAWSCredentials("cred1", "cred2");
	
	 public static class unzip extends DoFn<FileIO.ReadableFile, String> {
		 
		 @DoFn.ProcessElement
		 public void processElement(@Element ReadableFile element, DoFn.OutputReceiver<String> receiver) throws IOException {
			 AmazonS3 s3client = AmazonS3ClientBuilder
					  .standard()
					  .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
					  .withRegion(Regions.US_WEST_2)
					  .build();
			 System.out.println("Inside function ");
			 try {
			 InputStream is = Channels.newInputStream(element.open());
			 TarArchiveInputStream tis = new TarArchiveInputStream(is);
//			 File destFile = new File("s3://radiantlabs/sanfran-sfd0413c/results/simulations_untar/un_zip");
/*			 File destFile = new File("C:\\Users\\vpst1\\Downloads\\Compressed\\unzip");
	            
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
 */
			 TarArchiveEntry tarEntry = tis.getNextTarEntry();
		        while (tarEntry != null) {
		            byte[] btoRead = new byte[1024];
		            ByteArrayOutputStream bout = new ByteArrayOutputStream();
		            int len = 0;
		            while ((len = tis.read(btoRead)) != -1) {
		                bout.write(btoRead, 0, len);
		            }
		            bout.close();
		            String st=bout.toString();
		            ByteArrayInputStream bais =new ByteArrayInputStream(st.getBytes("UTF-8"));
		            Map<String, String> meta = new HashMap<String, String>();
		            meta.put("date", "02-05-2020");
		            ObjectMetadata medata = new ObjectMetadata(); 
		            medata.setUserMetadata(meta);
		            System.out.println("untar--"+tarEntry.getName());
		       s3client.putObject("radiantlabs", "sanfran-sfd0413c/results/simulations_untar/un_zip/"+tarEntry.getName(),bais,medata);
//		           minioClient.putObject("minio-001", "Unzip/"+tarEntry.getName(), bais, new PutObjectOptions(bais.available(), -1));
		            bais.close();
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
	  
	
	public interface CustomerOfflineDataOptions extends PipelineOptions {
		@Default.String("s3://radiantlabs/sanfran-sfd0413c/results/simulation_output/simulations_job1001.tar.gz")
		String getInputFile();

		void setInputFile(String file);
	}

	public static void main(String[] args) {
		CustomerOfflineDataOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(CustomerOfflineDataOptions.class);
		
		
		options.as(AwsOptions.class).setAwsCredentialsProvider(new AWSStaticCredentialsProvider(awsCredentials));
		options.as(AwsOptions.class).setAwsRegion("us-west-2");
		Pipeline pipeline = Pipeline.create(options);
		
		PCollection input = pipeline.apply("ReadInput", FileIO.match().filepattern(options.getInputFile()))
				.apply(FileIO.readMatches().withCompression(Compression.GZIP));
		input.apply(ParDo.of(new unzip()));

		pipeline.run().waitUntilFinish();
	}

}
