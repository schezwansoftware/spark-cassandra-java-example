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

    public PublisherPostStats(LocalDate updateddate, String publishername, Integer totalcount) {
        this.updateddate = updateddate;
        this.publishername = publishername;
        this.totalcount = totalcount;
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
                '}';
    }
}
