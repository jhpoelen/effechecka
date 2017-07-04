#!/bin/bash
../sbt/bin/sbt -mem 4096 -Dconfig.resource=/guoda.conf "run-main effechecka.WebApi"
