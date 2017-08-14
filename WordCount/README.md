## Word Count

#### Problem
count the frequency of words in files

#### Workflow
- Mapper: split each record (line) into words  
input: <offset, line>  
output: <word, 1>

- Reducer: sum up the count for each word  
input: <word, (1, 1, ...)>  
output: <word, count>

