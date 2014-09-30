sql-processing-engine
=====================

Simple SQL Processing Engine

Advanced Database System Course Project, there are three checkpoints for this project:

Checkpoint 1:
In this project, you will implement a simple SQL query evaluator with support for Select, Project, Join, Union, and Aggregate operations. That is, your code will receive a SQL file with a set of CREATE TABLE statements defining a schema for your data, and one or more SELECT statements.
Your code is expected to evaluate the SELECT statements on provided data, and produce output in a standardized form. Your code will be evaluated for both correctness and performance (in comparison to a naive evaluator based on volcano operators and nested- loop joins).


Checkpoint 2:
Enhance your query evaluator to provide full support for SQL, out-of-core query processing and limited forms of query evaluation.
First, this means that we'll be expecting a more feature-complete submission. Your code will be evaluated on a broader range of queries selected from TPC-H benchmark, which exercises a broader range of SQL features than the Project 1 test cases did.
Second, performance constraints will be tighter. The reference implementation for this project has been improved over that of Project 1, meaning that you'll be expected to perform more efficiently, and to handle data that does not fit into main memory.


Checkpoint 3:
Use a precomputation step to improve the performance of your system by gathering statistics, building indexes, etc...
With this project, you'll be given a 5 minute precomputation phase with which to improve your system's performance. Suggested uses for this additional time include building indexes as well as gathering table statistics. To help you, CREATE TABLE statements defining the schema will be extended with PRIMARY KEY and UNIQUE entries.
