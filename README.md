sql-processing-engine
=====================

Query Processing Engine (Course project of Database Systems) 

Check point 1
In this project, you will implement a simple SQL query evaluator with support for
Select, Project, Join, Union, and Aggregate operations. That is, your code will receive a
SQL file with a set of CREATE TABLE statements defining a schema for your data, and one
or more SELECT statements.
Your code is expected to evaluate the SELECT statements on provided data, and produce
output in a standardized form. Your code will be evaluated for both correctness and
performance (in comparison to a naive evaluator based on volcano operators and nestedloop
joins).

Check point 2
This project is, in effect, a more rigorous form of Check point 1. 
The requirements are identical: We give you a query and some data, you evaluate the query 
on the data and give us a response as quickly as possible.
First, this means that we'll be expecting a more feature-complete submission. Your code 
will be evaluated on a broader range of queries selected from TPC-H benchmark, which 
exercises a broader range of SQL features than the Project 1 test cases did.
Second, performance constraints will be tighter. 
The reference implementation for this project has been improved over that of Project 1, 
meaning that you'll be expected to perform more efficiently, and to handle data that does 
not fit into main memory.

Check point 3
to be continued...
