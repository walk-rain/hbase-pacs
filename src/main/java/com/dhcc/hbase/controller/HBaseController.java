package com.dhcc.hbase.controller;

import com.dhcc.hbase.service.HBaseService;
import com.dhcc.hbase.util.FileUtil;
import com.dhcc.hbase.util.ImageConvert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping(value = "/hbase")
public class HBaseController {

    @GetMapping(value = "/image", produces = MediaType.IMAGE_GIF_VALUE)
    @ResponseBody
    public byte[] test() {
        byte[] bytes = new byte[0];
        try {
            //bytes = ImageConvert.tiffTurnJpg2("E:/Test/1.tif");
            //File file = new File("E:/Test/1.tif");
            //FileInputStream inputStream = new FileInputStream(file);
            //byte[] ibytes = new byte[inputStream.available()];
            byte[] ibytes = FileUtil.getBytesByFile("D:/Test/1.tif");
            bytes = ImageConvert.tiffTurnJpg3(ibytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }
}
