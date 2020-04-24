CREATE TABLE ritam.publisherpoststats_date_bucket (
    updateddate date,
    totalcount int,
    publishername text,
    posttype text,
    PRIMARY KEY ((updateddate,posttype),totalcount, publishername)
) WITH CLUSTERING ORDER BY (totalcount DESC, publishername ASC);