#!/bin/bash
for i in `seq 4 9`
	do psql -f split_data_20160"$i"01.sql
done 
for i in `seq 10 12`
	do psql -f split_data_2016"$i"01.sql
done 
for i in `seq 1 9`
	do psql -f split_data_20170"$i"01.sql
done 
for i in `seq 10 12`
	do psql -f split_data_2017"$i"01.sql
done 
for i in `seq 1 2`
	do psql -f split_data_20180"$i"01.sql
done 
