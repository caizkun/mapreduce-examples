## Inverted Index

#### Problem
created an inverted index for looking up the words in files


#### Workflow
- Mapper: fetch the file name of each record and split the record into words  
input: <offset, line>  
output: <word, fileName>  

- Reducer: sum up the count for each word  
input: <word, (file1, file2, file1, ...)>  
output: <word, (file1=count1, file2=count2, ...)>


#### Note 
In this example, the driver implements the Tool interface and calls ToolRunner's static run() method to work in conjunction with GenericOptionsParser, which can parse the generic hadoop command-line arguments and modifies the Configuration of the Tool. 

Although it is unnecessary in this example, this driver format is very useful for customizing Configuration at run time by supplying command-line arguments. For example, the below command runs the program using two reduce tasks:
```bash
$ hadoop jar InvertedIndex.jar Driver -D mapreduce.job.reduces=2 /input /output
``` 


Reference: 
- *Hadoop: The Definitive Guide* (4th edition) by Tom White, p.148-152
- [Tool, ToolRunner in org.apache.hadoop.util](https://hadoop.apache.org/docs/stable/api/)
