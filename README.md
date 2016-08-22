[![Build Status](https://travis-ci.org/jhpoelen/effechecka.svg?branch=master)](https://travis-ci.org/jhpoelen/effechecka)

# effechecka
Generates taxonomic checklist and monitors biodiversity data access.

See http://effehecka.org or http://gimmefreshdata.github.io for a prototypes using this service.

# running
## standalone
to run the effechecka webservice:
```sh run-idigbio.sh```

## marathon
to run in http://github.com/mesosphere/marathon do something like
```curl -i -H 'Content-Type: application/json' -d@marathon.json [marathon host]:8082/v2/apps```



