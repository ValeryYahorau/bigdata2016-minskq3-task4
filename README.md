# bigdata2016-minskq3-task4
Task4: MapReduce jobs with secondary sort


###STEP 1 
Build project
```
mvn clean install
```

###STEP 2 
Run secondary sort job for logst with next command
```
yarn jar ~/bigdata2016-minskq3-task4/target/bigdata2016-minskq3-task4-1.0.0-jar-with-dependencies.jar com.epam.bigdata2016.minskq3.task4.SecondarySortJob <in> <out>
```
Where

`in` - path to input file/directory,

`out` - path for output,

e.g. 

yarn jar /root/Documents/bigdata2016-minskq3-task4/target/bigdata2016-minskq3-task4-1.0.0-jar-with-dependencies.jar com.epam.bigdata2016.minskq3.task4.SecondarySortJob /tmp/admin/in4.txt /tmp/admin/ou4.txt 


###Notes
- Was used counters to find iPinyou ID with the biggest amount of site-impression.
- Unit tests for job were implemeted.
- Also its possible to run job and unit tests like simple java applications.
