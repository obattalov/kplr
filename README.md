[![Go Report Card](https://goreportcard.com/badge/kplr-io/kplr)](https://goreportcard.com/report/kplr-io/kplr) [![Build Status](https://travis-ci.org/kplr-io/kplr.svg?branch=master)](https://travis-ci.org/kplr-io/kplr) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/kplr-io/kplr/blob/master/LICENSE)

# Kepler -  Journaling Database Management System
Kepler is a journaling database management system, which is built on top of the highly-performant Kepler journal. It is developed specifically for building distibuted, highly available, fault-tollerant and consistent log-data management systems.

Kepler is ideal for:
* log aggregation and log data management
* collecting data metrics and time statistics over it
* storing and blazingly fast processing of terabytes of data 
* running on premises, containerized, VM-based or as a cloud log-aggregation service

## Overview
The following diagram depicts two main Kepler's components - *Log Agent* and *Log Aggregator*, with data-flow between this two for a typical use-case:
![alt text](https://github.com/kplr-io/kplr/blob/master/docs/images/kplr-wkfl-dia.svg)

The *Log Agents* are intended for to be running on a Node(which is a host, VM, container or other OS driven unit), where logs of applications are initially written on the file-system. The *Log Agent* scans the applications log files, which usually stored in pretty konwn places (like `/var/log/` folder on Linux for example), reads the data from the log files, and sends it to the *Log Aggregator* using *Zebra* protocol. *Zebra* is secured, reliable, high-speed protocol developed for delivering logs to the *Log Aggregator* via Internet or through insecured networks. It has write-confirmation mechanism, what allows the *Log Agent* to be confident that the sent data is properly stored and it can send next portion then.

The *Log Aggregator* collects logs from multiple sources, persists them, and provides some user-friendly access to the data like - merging logs for shards of an application, filtering log messages, accessing to the logs by different criteria, paging, searching and log-streaming of the aggregated log data. KQL stays for *Kepler Query Language*, which is used for describing requests for log data provided by the *Log Aggregator*

Kepler is written in **Golang** and can be run on multiple platforms and environments. For example, Kepler has an "one-click" Kubernetes log aggregation system installation, which allows to have access to the log data to applications running in the Kubernetes cluster.

## Quick start
The steps below describe a quick way to try out what Kepler does. This section has purely demonstrative purpose, for more information on how to configure Kepler for production environment (on premise, in cloud, with kubernetes etc) please refer to the documentation (TBD.).

### Install prerequisites
- go 1.9+ (https://golang.org/doc/install)
- git (https://git-scm.com/downloads)

### Download, install and run Kepler
Create `/opt/kplr` folder and grant access for your user to the folder. This folder is needed for default configuration and you can easily change it via configuring kepler later, but for the demo, we ask to have it to make the things easy. 
Time to get the code:
```shell
go get github.com/kplr-io/kplr
cd $GOPATH/src/github.com/kplr-io/kplr
go get ./...
```
Now, when you have source code, just build and install Kepler components by:
```shell
go install -v ./...
```
The command above compile 2 executables for you platform. If everything went fine, you will have `kplr-agent` for *Log Agent* and `kplr` executable file for *Log Aggregator*. 
Open a console and run the *Log Aggregator*:
```shell
kplr
```
The command above will run the *Log Aggregator* with some default configuration. It is also ready to serve HTTP requests on port 8080 by default. You can test that the *Log Aggregator* runs, just typing the following in another console:
```shell
curl localhost:8080/ping
```
The expected response should be `pong`, if you see it, the *Log Aggregator* runs normally. 
Run the *Log Agent* in another console, just type:
```shell
kplr-agent
```
You will see some logs from the *Agent*. If everything goes fine, now the agent collects logs from default path (/var/log/...) and sends the information to the *Log Aggregator*. You can check, what it found, making the following query to the *Log Aggregator*:
```shell
curl 127.0.0.1:8080/journals
{"data":["corecaptured.log","displaypolicyd.IGPU.log","displaypolicyd.log","displaypolicyd.stdout.log", "system.log"...
```
If you see not empty "data" list, you have some journals aggregated! Just type the following query for a journal from the list (system.log in the example below):
```shell
curl '127.0.0.1:8080/logs?__source_id__="system.log"&blocked=false&offset=10&position=tail'
```
The query should return you 10 last log records from "system.log" file, which is collected in your *Log Aggregator* now. 
