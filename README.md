# Anacostia
Welcome to Anacostia. Anacostia is a framework for creating machine learning operations (MLOps) pipelines. I believe the process of creating MLOps pipelines today are too difficult; thus, this is my attempt at simplifying the entire process. 
This is a pipeline which is used to validate, clean and encrypt the incoming new data to the LLM

## Notes for contributors and developers
If you are interested in contributing to Anacostia, please see CONTRIBUTORS.md. 
If you are interested in building your own plugins for Anacostia and contributing to the Anacostia ecosystem, please see DEVELOPERS.md. 

## Basic Anacostia Concepts & Terminology:
Anacostia works by allowing you to define a pipeline as a directed acyclic graph (DAG). Each node in the DAG is nothing more than a continuously running thread that does the following:
1. Waits for enough data to become available in a resource or waits for signals recieved from other nodes.
2. Executes a job. 
3. Send signal to another node upon completion of its job.

The edges of the DAG dictates which child nodes are listening for signals from which parent nodes.

There are fundamentally three types of nodes in Anacostia:
1. Metadata store nodes: stores tracking information about each time the pipeline executes (i.e., a *run*).
    - The metadata store is responsibles for storing information like the start/end time of the run, metadata information about all the nodes in the pipeline, etc. 
    - All metadata store nodes must implement the following methods: ...
2. Resource nodes: think of a "resource" as the inputs and outputs of your pipeline.
    - A resource can be a folder on a local filesystem, an S3 bucket, an API endpoint, a database, etc.
    - An "input resource" is a resource that is monitored for changes. When there is enough data in the input resource, it triggers the pipeline to start executing.
    - An "output resource" is a resource that is not monitored for changes. This is a resource that stores artifacts produced by the pipeline. 
