[![Build Status](https://travis-ci.org/jhpoelen/effechecka.svg?branch=master)](https://travis-ci.org/jhpoelen/effechecka)

# effechecka
Taxon checklist generator: creates a list of organisms that satisfy specified spatial, temporal or trait based constraints.

See gh_pages branch and http://jhpoelen.github.io/effechecka for a prototype of the checklist generator.

# running
## standalone
to run the effechecka webservice:
```sh run-idigbio.sh```

## marathon
```curl -i -H 'Content-Type: application/json' -d@effechecka-marathon.json [marathon host]:8082/v2/apps```



