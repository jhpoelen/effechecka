#!/bin/bash
sbt run \
-Deffechecka.host=0.0.0.0 \
-Deffechecka.data.dir=/home/int/data/idigbio/ \
-Deffechecka.spark.master.url=mesos://127.0.1.1:7077 \
-Deffechecka.spark.job.jar=/home/int/idigbio-spark/target/scala-2.10/iDigBio-LD-assembly-1.1.jar
