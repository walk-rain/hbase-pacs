package com.dhcc.hbase.util;

import com.sun.media.jai.codec.*;

import javax.media.jai.JAI;
import javax.media.jai.RenderedOp;
import java.io.*;

public class ImageConvert {
    public static void tiffTurnJpg(String filePath) {
        RenderedOp file = JAI.create("fileload", filePath);

        OutputStream ops = null;
        try {
            ops = new FileOutputStream("E:/Test/1.jpg");
            //文件存储输出流
            JPEGEncodeParam param = new JPEGEncodeParam();
            ImageEncoder image = ImageCodec.createImageEncoder("JPEG", ops, param); //指定输出格式
            //解析输出流进行输出
            image.encode(file);
            //关闭流
            ops.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("tiff转换jpg成功");
    }

    public static byte[] tiffTurnJpg2(String filePath) throws IOException {
        RenderedOp file = JAI.create("fileload", filePath);
        OutputStream ops = null;
        ops = new ByteArrayOutputStream();
        //文件存储输出流
        JPEGEncodeParam param = new JPEGEncodeParam();
        ImageEncoder image = ImageCodec.createImageEncoder("JPEG", ops, param); //指定输出格式
        //解析输出流进行输出
        image.encode(file);
        //关闭流
        ops.close();
        return ((ByteArrayOutputStream) ops).toByteArray();
    }

    public static byte[] tiffTurnJpg3(byte[] bytes) throws IOException {
        InputStream is = new ByteArrayInputStream(bytes);
        FileCacheSeekableStream stream = new FileCacheSeekableStream(is);
        RenderedOp in = JAI.create("stream", stream);
        OutputStream os = new ByteArrayOutputStream();
        JPEGEncodeParam param = new JPEGEncodeParam();
        ImageEncoder enc = ImageCodec.createImageEncoder("JPEG", os, param);
        enc.encode(in);
        os.close();
        stream.close();
        return ((ByteArrayOutputStream) os).toByteArray();
    }

    public static void main(String[] args) {
        tiffTurnJpg("E:/Test/1.tif");
    }
}
