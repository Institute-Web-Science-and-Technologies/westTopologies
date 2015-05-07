# westTopology
## Installation instructions
run `mvn initialize` in order to install third-party jars into your local maven repository

## Creating a new topology
* Choose a name for the topology and create a folder with this name inside this project (the name should be URL compatible)
* Each topology has to be an own maven module of this project and therefore has to be listed in the [parent POM](https://github.com/Institute-Web-Science-and-Technologies/westTopologies/blob/master/pom.xml)
* Examples of topology modules with an existing pom are available [here](https://github.com/Institute-Web-Science-and-Technologies/westTopologies/tree/master/lodExplorerTopology) and [here](https://github.com/Institute-Web-Science-and-Technologies/westTopologies/tree/master/roleAnalysisTopology)

## Implementing a storm bolt
* Sample implementations are available [here](https://github.com/Institute-Web-Science-and-Technologies/westTopologies/tree/master/roleAnalysisTopology/src/main/java/uniko/west/topology/bolts) and [here](https://github.com/Institute-Web-Science-and-Technologies/westTopologies/tree/master/lodExplorerTopology/src/main/java/uniko/west/topology/bolts)
* Some of these samples use static files for their computations. All static files which are used by a bolt have to be moved into the [resources directory of the REST service](https://github.com/Institute-Web-Science-and-Technologies/reveal_restlet/tree/master/resources) in order to be available at runtime. Every file which is located in this directory will be accessible via http at http://localhost:8182/static/${filename} (replace localhost by the REST service's server address). 

## Deploying Topologies
* Deployment without using the REST service
  * run `mvn --projects someTopology clean install` in order to deploy someTopology to the storm cluster
* Deployment with the REST service
  * Follow the instructions of the [REST service](https://github.com/Institute-Web-Science-and-Technologies/reveal_restlet)
