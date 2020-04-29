package com.codesetters.domain;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.UUID;

@Table(name = "publisherpoststats")
public class PublisherPostStats implements Serializable {

    private LocalDate updateddate;

    private String publishername;

    private Integer totalcount;

    private String posttype;

    private Integer year;

    private Integer month;

    public String getPublishername() {
        return publishername;
    }

    public void setPublishername(String publishername) {
        this.publishername = publishername;
    }

    public Integer getTotalcount() {
        return totalcount;
    }

    public void setTotalcount(Integer totalcount) {
        this.totalcount = totalcount;
    }

    public PublisherPostStats() {
    }

    public PublisherPostStats(LocalDate updateddate, String publishername, Integer totalcount,String posttype,Integer year,Integer month) {
        this.updateddate = updateddate;
        this.publishername = publishername;
        this.totalcount = totalcount;
        this.posttype = posttype;
        this.month = month;
        this.year = year;
    }

    public LocalDate getUpdateddate() {
        return updateddate;
    }

    public void setUpdateddate(LocalDate updateddate) {
        this.updateddate = updateddate;
    }

    @Override
    public String toString() {
        return "PublisherPostStats{" +
                ", publishername='" + publishername + '\'' +
                ", totalcount='" + totalcount + '\'' +
                ", posttype='" + posttype + '\'' +
                ", year='" + year + '\'' +
                ", month='" + month + '\'' +
                '}';
    }

    public String getPosttype() {
        return posttype;
    }

    public void setPosttype(String posttype) {
        this.posttype = posttype;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }
}
