package com.dhcc.hbase.dto;

import java.util.ArrayList;

public class PacsDTO {
    private ArrayList<String> colums;
    private ArrayList<String> values;
    private byte[] fileContent;

    public ArrayList<String> getColums() {
        return colums;
    }

    public void setColums(ArrayList<String> colums) {
        this.colums = colums;
    }

    public ArrayList<String> getValues() {
        return values;
    }

    public void setValues(ArrayList<String> values) {
        this.values = values;
    }

    public byte[] getFileContent() {
        return fileContent;
    }

    public void setFileContent(byte[] fileContent) {
        this.fileContent = fileContent;
    }
}
