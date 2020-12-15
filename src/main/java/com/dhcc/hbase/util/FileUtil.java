package com.dhcc.hbase.util;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

public class FileUtil {
    public static String ReadFile(File file) {
        FileReader fr = null;
        String fileContent = null;
        try {
            StringBuffer sb = new StringBuffer();
            fr = new FileReader(file);
            // 定义字符数组
            char[] buf = new char[1024];// 缓冲区大小
            int len = 0;// 长度读取的字符个数
            while ((len = fr.read(buf)) != -1) {
                sb.append(buf,0,len);
            }
            fileContent = new String(sb);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fr != null) {
                try {
                    fr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return fileContent;
    }

    public static byte[] getBytesByFile(String pathStr) {
        File file = new File(pathStr);
        try {
            FileInputStream fis = new FileInputStream(file);
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
            byte[] b = new byte[1024];
            int n;
            while ((n = fis.read(b)) != -1) {
                bos.write(b, 0, n);
            }
            fis.close();
            byte[] data = bos.toByteArray();
            bos.close();
            return data;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void test() throws IOException {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream("D:\\Test\\input\\1.tif"));
        StringBuffer sb = new StringBuffer();
        int c = -1;
        while ((c=bufferedInputStream.read())!=-1) {
            //将读取的文件以字节方式写入DataOutputStream，之后传输到服务端
            //这里也可以使用byte[]数据进行读取和写入
            sb.append(c);
        }
        System.out.println(sb.length());
        System.out.println(sb.toString());
    }

    public static void main(String[] args) throws IOException {
        //String fileContent = ReadFile(new File("D:\\Test\\input\\1.tif"));
        //System.out.println(fileContent.length());
        //System.out.println(fileContent);
        //37216
        //test();
        //99467
        /*
        File file = new File("D:\\Test\\input\\1.tif");
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
        ByteArrayOutputStream bAOutputStream = new ByteArrayOutputStream();
        int c = -1;
        while ((c=bufferedInputStream.read())!=-1) {
            System.out.print(c);
            bAOutputStream.write(c);
        }
        System.out.println();
        byte data [] = bAOutputStream.toByteArray();
        bAOutputStream.close();
        for (byte b1 : data) {
            System.out.print(b1);
        }
        System.out.println();
        System.out.println(data.length);
        byte[] bytes = Bytes.toBytes(3);
        for (byte b2 : bytes) {
            System.out.print(b2 + " ");
        }*/
        byte[] bytes = new byte[0];
        byte[] ibytes = FileUtil.getBytesByFile("D:\\Test\\input\\1.tif");
        bytes = ImageConvert.tiffTurnJpg3(ibytes);
        System.out.println(ibytes.length);
        for (byte b1 : ibytes) {
            System.out.print(b1);
        }
    }
}
