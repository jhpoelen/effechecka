#!/bin/bash
if [ $# -eq 0 ]; then
  echo "usage: $0 [mailgun api key]";
  echo " ";
  exit 1;
fi

sbt "run-main effechecka.SubscriptionNotifier" \
-Deffechecka.mailgun.apikey=$1
