#!/bin/bash
sbt "run-main effechecka.WebApi" \
-Deffechecka.host=0.0.0.0 \
-Deffechecka.data.dir=/home/int/data \
-Deffechecka.spark.master.host=api.effechecka.org \
-Deffechecka.spark.master.port=7077 \
-Deffechecka.spark.master.url=mesos://api.effechecka.org:7077 \
-Deffechecka.spark.job.jar=/home/int/jobs/iDigBio-LD-assembly-latest.jar
