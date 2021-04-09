package com.dataengine.spark.core.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * hdfs 文件合并工具
 * create by Roy
 * 2019/04/02
 */
public class CFileUtils {

    /**
     * Transfer to GBK, directly return the byte stream, used for the flow of mail,
     * there is no need to save temporary files
     * Copy all files in a directory to one output file (merge).
     */
    public static byte[] copyMerge(FileSystem srcFS, Path srcDir,
                                   boolean deleteSource) throws IOException {
        byte[] byteArray = null;
//        dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);
        if (!srcFS.getFileStatus(srcDir).isDirectory())
            return null;
        //OutputStream out = dstFS.create(dstFile);
        try {
            FileStatus contents[] = srcFS.listStatus(srcDir);
            Arrays.sort(contents);
            for (int i = 0; i < contents.length; i++) {
                if (contents[i].isFile()) {
                    InputStream in = srcFS.open(contents[i].getPath());
                    byte[] baos = cloneInputStream(in);
                    byteArray = baos;
//                        if (addString != null)
//                            out.write(addString.getBytes("GBK"));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (deleteSource) {
            srcFS.delete(srcDir, true);
        }
        return byteArray;
    }

    public static String copyMerge2(FileSystem srcFS, Path srcDir,
                                    boolean deleteSource,
                                    Configuration conf, String addString, String filename) throws IOException {
        return copyMerge3(srcFS, srcDir, deleteSource, conf, addString, filename);
    }

    /**
     * Transcoding, first in the local compression, save to the local temporary file,
     * after the operation needs to manually clear the file
     *
     * @param srcFS        hdfs
     * @param srcDir       hdfs File path on
     * @param deleteSource Whether to delete the original file
     * @param conf         hdfs config
     * @param addString    addString="id,name,age+\n",Write in the file file header, can be used as a column title, example: id,name,age, need to add a new line attached
     * @param filename     Save the local file name, the file name needs to use the original file timestamp to define,
     *                     to avoid conflicts, write in the later optimization
     * @return 文件路径
     * @throws IOException
     */
    public static String copyMerge2(FileSystem srcFS, Path srcDir,
                                    boolean deleteSource,
                                    Configuration conf, String addString, String filename, int type) throws IOException {
        String filezipPath = "";
//        dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);
        if (!srcFS.getFileStatus(srcDir).isDirectory())
            return null;
//        OutputStream out = dstFS.create(dstFile);
        String os = System.getProperty("os.name");
        String tmpPath = "/tmp/file_zip_tmp/" + filename + ".zip";
        if (os.toLowerCase().startsWith("win")) {
            tmpPath = "E:" + tmpPath;
        }
        File zipFile = new File(tmpPath);
        if (!zipFile.exists()) {
            if (!zipFile.getParentFile().exists()) {
                zipFile.getParentFile().setExecutable(true, false);
                zipFile.getParentFile().setWritable(true, false);
                zipFile.getParentFile().mkdirs();
            }
            System.out.println("createNewFile start== " + tmpPath);
            zipFile.createNewFile();
//            zipFile.setWritable(true);

        }
        ZipOutputStream output = new ZipOutputStream(new FileOutputStream(zipFile));
        output.putNextEntry(new ZipEntry(filename));
        if (addString != null) {
            if (!addString.endsWith("\n")) {
                addString += "\n";
            }
            output.write(addString.getBytes("GBK"));
        }
        try {
            FileStatus contents[] = srcFS.listStatus(srcDir);
            Arrays.sort(contents);
            for (int i = 0; i < contents.length; i++) {
//                System.out.println("contents[i].getName()==" + contents[i].getPath().getName() + " end ");
                if (contents[i].isFile() && contents[i].getPath().getName().endsWith(".csv")) {
//                    System.out.println("contents[i].getName() csv==" + contents[i].getPath().getName() + " end ");
                    InputStream in = null;
                    // 打开个新的输入流
                    InputStream new_stream_in = null;
                    try {
                        in = srcFS.open(contents[i].getPath());
                        byte[] baos = cloneInputStream(in);
                        new_stream_in = new ByteArrayInputStream(baos);
                        //conf.getInt("io.file.buffer.size", 4096)
                        cCopyBytes(new_stream_in, output, 4096, false);
//                        filezipPath = zipFile.getPath();
                    } catch (Exception e) {
                        System.out.println(" for Exception " + e.getMessage() + " end ");
                        e.printStackTrace();
                    } finally {
                        if (in != null) {
                            new_stream_in.close();
                        }
                        if (new_stream_in != null) {
                            in.close();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            output.close();
            // out.close();
        }
        if (deleteSource) {
            srcFS.delete(srcDir, true);
        }
        return tmpPath;
    }


    public static void inputstreamtofile(InputStream ins, File file) throws Exception {

        OutputStream os = new FileOutputStream(file);
        int bytesRead = 0;
        byte[] buffer = new byte[8192];
        while ((bytesRead = ins.read(buffer, 0, 4096)) != -1) {
            os.write(buffer, 0, bytesRead);
        }
        os.close();
        ins.close();
    }


    /**
     * Copies from one stream to another.
     *
     * @param in       InputStrem to read from
     * @param out      OutputStream to write to
     * @param buffSize the size of the buffer
     */
    public static void cCopyBytes(InputStream in, OutputStream out, int buffSize, boolean close)
            throws IOException {
        try {
            // PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
            byte buf[] = new byte[buffSize];
            int bytesRead = in.read(buf);
            while (bytesRead >= 0) {
                out.write(buf, 0, bytesRead);
                out.flush();
//                if ((ps != null) && ps.checkError()) {
//                    throw new IOException("Unable to write to output stream.");
//                }
                bytesRead = in.read(buf);
            }
            //
            if (close) {
                out.close();
                out = null;
                in.close();
                in = null;
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (close) {
                IOUtils.cleanup(null, out);
                IOUtils.cleanup(null, in);
            }
        }

    }


    /**
     * 改变字符编码
     *
     * @param input
     * @return
     */
    private static byte[] cloneInputStream(InputStream input) {
        try {
//            BufferedInputStream bis = new BufferedInputStream(input);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int len;
            while ((len = input.read(buffer)) > 0) {
//                String input_str=new String(buffer,"gbk");
//                byte[] newBuffer = new String(buffer).getBytes("gbk");
                baos.write(buffer, 0, len);
            }
            baos.flush();
            byte[] newByte = new String(baos.toByteArray()).getBytes("gbk");
            return newByte;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static Path checkDest(String srcName, FileSystem dstFS, Path dst,
                                  boolean overwrite) throws IOException {
        if (dstFS.exists(dst)) {
            FileStatus sdst = dstFS.getFileStatus(dst);
            if (sdst.isDirectory()) {
                if (null == srcName) {
                    throw new IOException("Target " + dst + " is a directory");
                }
                return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
            } else if (!overwrite) {
                throw new IOException("Target " + dst + " already exists");
            }
        }
        return dst;
    }


    /**
     * @param srcFS
     * @param srcDir
     * @param deleteSource
     * @param conf
     * @param addString
     * @param filename
     * @return
     * @throws IOException
     */
    public static String copyMerge3(FileSystem srcFS, Path srcDir,
                                    boolean deleteSource,
                                    Configuration conf, String addString, String filename) throws IOException {
        String filezipPath = "";
//        dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);
        if (!srcFS.getFileStatus(srcDir).isDirectory())
            return null;
//        OutputStream out = dstFS.create(dstFile);
        String os = System.getProperty("os.name");
        String tmpPath = "/tmp/file_zip_tmp/" + filename + ".zip";
        if (os.toLowerCase().startsWith("win")) {
            tmpPath = "E:" + tmpPath;
        }
        File zipFile = new File(tmpPath);
        if (!zipFile.exists()) {
            if (!zipFile.getParentFile().exists()) {
                zipFile.getParentFile().setExecutable(true, false);
                zipFile.getParentFile().setWritable(true, false);
                zipFile.getParentFile().mkdirs();
            }
            System.out.println("createNewFile start== " + tmpPath);
            zipFile.createNewFile();
//            zipFile.setWritable(true);

        }
        ZipOutputStream output = new ZipOutputStream(new FileOutputStream(zipFile));
        ZipEntry zipEntry = new ZipEntry(filename + ".csv");
        output.putNextEntry(zipEntry);
        if (addString != null) {
            if (!addString.endsWith("\n")) {
                addString += "\n";
            }
            output.write(addString.getBytes("GBK"));
        }
        InputStream in = null;
        InputStream new_stream_in = null;
        String new_stream_in_title = "";
        BufferedReader bf = null;
        try {
            FileStatus contents[] = srcFS.listStatus(srcDir);
            Arrays.sort(contents);
            int line = 0;
            int txtSize = 500000;
            int version = 1;
            for (int i = 0; i < contents.length; i++) {
//                System.out.println("contents[i].getName()==" + contents[i].getPath().getName() + " end ");
                if (contents[i].isFile() && contents[i].getPath().getName().endsWith(".csv")) {
//                    System.out.println("contents[i].getName() csv==" + contents[i].getPath().getName() + " end ");

                    try {
                        in = srcFS.open(contents[i].getPath());
                        byte[] baos = cloneInputStream(in);
                        bf = new BufferedReader(new InputStreamReader(srcFS.open(contents[i].getPath()), "UTF-8"));
                        String tempString = null;


                        while ((tempString = bf.readLine()) != null) {
                            int txtNo = line / txtSize + 1;
                            new_stream_in = new ByteArrayInputStream((tempString + "\n").getBytes("GBK"));
                            line++;
                            if (line == 1) {
                                new_stream_in_title = tempString + "\n";
                            }

                            if (line / txtSize > 0 && txtNo > version) {
                                output.closeEntry();
                                zipEntry = new ZipEntry(filename + "_" + txtNo + ".csv");
                                output.putNextEntry(zipEntry);
                                if (addString == null) {
                                    cCopyBytes(new ByteArrayInputStream(new_stream_in_title.getBytes("GBK")), output, 4096, false);
                                } else {
                                    if (!addString.endsWith("\n")) {
                                        addString += "\n";
                                    }
                                    output.write(addString.getBytes("GBK"));
                                }
                                version++;
                            }
                            cCopyBytes(new_stream_in, output, 4096, false);
                        }


                    } catch (Exception e) {
                        System.out.println(" for Exception " + e.getMessage() + " end ");
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            output.close();
            // out.close();
            if (new_stream_in != null) {
                new_stream_in.close();
            }
            if (in != null) {
                in.close();
            }
            if (bf != null) {
                bf.close();
            }
        }
        if (deleteSource) {
            srcFS.delete(srcDir, true);
        }
        return tmpPath;
    }

}
