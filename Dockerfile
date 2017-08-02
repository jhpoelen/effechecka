FROM openjdk:8-jre-alpine

RUN apk add --update bash libc6-compat

ENV sbt_version=0.13.15

WORKDIR /usr/local
ADD https://github.com/sbt/sbt/releases/download/v${sbt_version}/sbt-${sbt_version}.zip /usr/local/sbt-${sbt_version}.zip
RUN unzip sbt-${sbt_version}.zip && rm sbt-${sbt_version}.zip
ADD . /usr/local/effechecka-master/
WORKDIR /usr/local/effechecka-master
RUN /usr/local/sbt/bin/sbt compile

CMD ["/usr/local/sbt/bin/sbt","-mem","4096","-Dconfig.resource=guoda.conf", "compile", "run"]
