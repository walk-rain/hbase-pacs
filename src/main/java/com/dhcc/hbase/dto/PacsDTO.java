package com.dhcc.hbase.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;

@Setter
@Getter
@ToString
public class PacsDTO {
    private ArrayList<String> colums;
    private ArrayList<String> values;
    private byte[] fileContent;
}
