#!/bin/bash
sbt "run-main effechecka.WebApi" \
-Deffechecka.host=0.0.0.0 \
-Deffechecka.port=8889 \
-Deffechecka.data.dir=hdfs://mesos02.acis.ufl.edu/guoda/data/ \
-Deffechecka.spark.master.url=mesos://api.effechecka.org:7077 \
-Deffechecka.spark.job.jar=/home/int/jobs/iDigBio-LD-assembly-1.5.5.jar
