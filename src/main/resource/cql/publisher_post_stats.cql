CREATE TABLE ritam.publisherpoststats_date_bucket (
    updateddate date,
    totalcount int,
    publishername text,
    posttype text,
    month int,
    year int,
    PRIMARY KEY ((updateddate,posttype),totalcount, publishername)
) WITH CLUSTERING ORDER BY (totalcount DESC, publishername ASC);

CREATE TABLE ritam.publisherpoststats_by_month (
    updateddate date,
    totalcount int,
    publishername text,
    posttype text,
    month int,
    year int,
    PRIMARY KEY ((year,month),posttype, publishername)
) WITH CLUSTERING ORDER BY (posttype ASC ,publishername ASC);