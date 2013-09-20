<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

Auto-Scaling with Apache Helix and Apache YARN
------------------------
Auto-scaling for helix clusters using a managed and a meta cluster. The managed cluster operates as usual, managing resources and instances via AUTO_REBALANCE. The meta cluster monitors the managed cluster and injects or removes instances based on demand.

The meta cluster makes decisions about scaling up or down based on information obtained from a "ClusterStatusProvider". A custom "ProviderRebalancer" is invoked testing the health of existing participants in the managed cluster with the "ContainerStatusProvider". If participants need to be (re-)deployed the "ContainerProvider" is invoked to instantiate and inject participants in the managed cluster.

ContainerProviders are the participants of the meta cluster and there are multiple different implementations of the "ContainerProvider". First, the "LocalContainerProvider" spawns VM-local participants, i.e. participants of the managed cluster are spawned in the same VM the container provider exists. This is mainly useful for testing. Second, the "ShellContainerProvider" spawns a separate VM process for each participant using shell commands. Third, the "YarnContainerProvider" creates processes as container on a YARN cluster and manages their status using an external meta-data service (Zookeeper in this implementation). This implementation is fairly complex and has a number of external dependencies on a working YARN cluster and running services.

Even though there are different types of providers the notion of a "ContainerProcess" abstracts implementation specifics. A process implementation inherits from "ContainerProcess" and can be instantiated by all three types of container providers. CAUTION: since separate VM process might be used a VM external method for coordination is required (e.g. Zookeeper)

Configuration settings are passed throughout the application using traditional Properties objects. The "ConfigTool" contains default paths and helps to inject dependencies in the ProviderRebalancer.

The application can be run and tested in two ways. First, a comprehensive suite of unit and integration tests can be run using "mvn verify". Second, the "Bootstrapper" can deploy a live managed and meta cluster based on a specification (e.g. "2by2shell.properties"). 

------------------------
The IdealState of the meta cluster uses the ONLINE-OFFLINE model and maps as follows in the example below:

Resource: type of container, e.g. database, webserver
Partition: container id
Instance: responsible container provider

META:

database
  database_0
    provider_0 : ONLINE
  database_1
    provider_1 : ONLINE
webserver
  webserver_0
    provider_0 : ONLINE
  webserver_1
    provider_1 : ONLINE
  webserver_2
    provider_0 : ONLINE

      
MANAGED:

dbprod (tag=database)
  dbprod_0
    database_0 : MASTER
    database_1 : SLAVE
  dbprod_1
    database_0 : SLAVE
    database_1 : MASTER
  dbprod_2
    database_0 : MASTER
    database_1 : SLAVE
wsprod (tag=webserver)
  wsprod_0
    webserver_0 : ONLINE
  wsprod_1
    webserver_1 : ONLINE
  wsprod_2
    webserver_2 : ONLINE
  wsprod_3
    webserver_0 : ONLINE
  wsprod_4
    webserver_1 : ONLINE
  wsprod_5
    webserver_2 : ONLINE
    