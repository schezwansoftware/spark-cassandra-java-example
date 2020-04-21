FROM bde2020/spark-submit:latest

LABEL maintainer="Gezim Sejdiu <g.sejdiu@gmail.com>, Giannis Mouchakis <gmouchakis@gmail.com>"

ENV SPARK_APPLICATION_JAR_NAME spark-streaming-word-count-1.0-SNAPSHOT.jar
ENV SPARK_APPLICATION_MAIN_CLASS com.codesetters.Main
ENV SPARK_APPLICATION_ARGS ""
ENV SPARK_APPLICATION_JAR_LOCATION=/usr/src/app/target/spark-streaming-word-count-1.0-SNAPSHOT.jar
COPY template.sh /

RUN apk add --no-cache openjdk8 \
      && chmod +x /template.sh \
      && mkdir -p /app \
      && mkdir -p /usr/src/app

WORKDIR /usr/src/app

COPY . .

CMD ["/bin/bash", "/template.sh"]
