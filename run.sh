#!/bin/bash
sbt "run-main effechecka.WebApi" \
-Deffechecka.host=0.0.0.0 \
-Deffechecka.data.dir=/home/int/data/ \
-Deffechecka.spark.master.url=mesos://apihack-c18.idigbio.org:7077 \
-Deffechecka.spark.job.jar=/home/int/jobs/iDigBio-LD-assembly-1.5.1.jar
