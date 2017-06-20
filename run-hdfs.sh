#!/bin/bash
sbt "run-main effechecka.WebApi" \
-Deffechecka.host=0.0.0.0 \
-Deffechecka.port=8889 \
-Deffechecka.data.dir=hdfs:/guoda/data/gbif-idigbio.parquet/ \
-Deffechecka.spark.master.url=mesos://mesos07.acis.ufl.edu:7077 \
-Deffechecka.spark.job.jar=hdfs:/guoda/lib/iDigBio-LD-assembly-latest.jar
