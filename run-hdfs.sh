#!/bin/bash
../sbt/bin/sbt "run-main effechecka.WebApi" \
-Deffechecka.host=10.13.44.21 \
-Deffechecka.port=8889 \
-Deffechecka.cassandra.host=apihack-c18.idigbio.org \
-Deffechecka.data.dir=hdfs://localhost:9000/guoda/data/gbif-idigbio.parquet/ \
-Deffechecka.spark.master.url=mesos://mesos07.acis.ufl.edu:7077 \
-Deffechecka.spark.job.jar=hdfs://localhost:9000/guoda/lib/iDigBio-LD-assembly-latest.jar
