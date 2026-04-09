package de.metanome.backend.algorithm_loading;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Uploads file to directory passed as argument
 */
public class FileUpload {

    /**
     * Function to upload file to directory passed as argument
     *
     * @param uploadedInputStream InputStream of file send
     * @param fileDetail additional MetaData about uploaded file
     * @param targetDirectory directory were file should be stored
     * @return if file already existed
     **/
    public boolean writeFileToDisk(InputStream uploadedInputStream,
                                FormDataContentDisposition fileDetail,
                                String targetDirectory) throws IOException  {

            if (targetDirectory == null) {
                throw new IOException("Target directory is null");
            }
            String dir = targetDirectory;
            System.out.println("Target directory for file upload not normalized: " + dir);
            // Normalize Windows path that may start with "/C:/" style
            if (System.getProperty("os.name").toLowerCase().contains("win") && dir.matches("^/[A-Za-z]:/.*")) {
                dir = dir.substring(1);
                System.out.println("Adjusted Windows path for file upload: " + dir);
            }
            Path targetDirPath = Paths.get(dir).normalize();
            System.out.println("Normalized target directory path for file upload: " + targetDirPath.toString());
            Files.createDirectories(targetDirPath);
            Path filePath = targetDirPath.resolve(fileDetail.getFileName()).normalize();

            boolean file_exist = Files.deleteIfExists(filePath);

            byte[] buffer = new byte[8192];
            int read;
            try (OutputStream out = Files.newOutputStream(filePath)) {
                while ((read = uploadedInputStream.read(buffer)) != -1) {
                    out.write(buffer, 0, read);
                }
                out.flush();
            }
            return file_exist;


    }
}
