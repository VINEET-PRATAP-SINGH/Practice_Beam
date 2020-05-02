package minIO_practice;

import io.minio.MinioClient;
import io.minio.PutObjectOptions;
import io.minio.errors.MinioException;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;


public class MinIOUnzip {

	public static void main(String[] args) throws IOException, NoSuchAlgorithmException, InvalidKeyException {
		  try {
		      
		      MinioClient minioClient =new MinioClient("https://play.min.io","Q3AM3UQ867SPQQA43P2F","zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG");
		      minioClient.statObject("minio-001", "simulations_job0.tar.gz");
		      InputStream stream = minioClient.getObject("minio-001", "simulations_job0.tar.gz");

		      GZIPInputStream gZIPInputStream = new GZIPInputStream(stream);
		      TarArchiveInputStream tis = new TarArchiveInputStream(gZIPInputStream);

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
		            
		           
		            System.out.println("untar--"+tarEntry.getName());
		            minioClient.putObject("minio-001", "Unzip/"+tarEntry.getName(), bais, new PutObjectOptions(bais.available(), -1));
		            bais.close();
		            tarEntry = tis.getNextTarEntry();
		        }
		        System.out.println("done");
		        tis.close();
		      		tis.close();
		            gZIPInputStream.close();
		      stream.close();
		    } catch (MinioException e) {
		      System.out.println("Error occurred: " + e);
		    }
	}

}
