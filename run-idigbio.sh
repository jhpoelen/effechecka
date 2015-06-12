nohup sbt run \
-Deffechecka.host=0.0.0.0 \
-Deffechecka.data.dir=/home/int/data/idigbio/ \
-Deffechecka.spark.master.url=spark://c18node15:6066 \
-Deffechecka.spark.job.jar=/home/int/idigbio-spark/target/scala-2.10/iDigBio-LD-assembly-1.0.jar \
&> /home/int/effechecka/target/effechecka.log &
