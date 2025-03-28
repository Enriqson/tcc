FROM apache/hive:4.0.1

ENV SERVICE_NAME=metastore

# Copy only requirements to cache them in docker layer
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
RUN me="$USER"
USER root

RUN apt-get update -y && \ 
    apt-get install -y wget

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar -O /opt/hive/lib/hadoop-aws-3.3.6.jar

RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar -O /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar

USER $me
