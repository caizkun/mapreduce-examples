## Matrix-Vector Multiplication

#### Problem
Calculate the product of a matrix *M* (assumed sparse) and a vector *v*

Here we show a toy example  
![toy_example](./toy_example.png)  


#### Workflow - 2 MapReduce jobs
1. CellMultiplication: Mapper1 + Mapper2 ---> Reducer  
    - Mapper1: read the non-zero elements in the matrix file (row, col, *M*[row][col])  
        input: <offset, line>  
        output: <col, row=*M*[row][col]>
    - Mapper2: read the vector  
        input: <offset, line>  
        output: <row, *v*[row]>
    - Reducer: multiply a matrix column with the corresponding vector row  
        input: <col, (row1=*M*[row1][col], row2=*M*[row2][col], ..., *v*[col])>  
        output: <row, *M*[row][col]**v*[col]>

2. CellSum: Mapper ---> Reducer
    - Mapper: read the intermediate result of cell multiplication  
        input: <offset, line>  
        output: <row, *M*[row][col]**v*[col]> 
    
    - Reducer: sum up all the cell product to the final value for each vector row  
        input: <row, (*M*[row][col1]**v*[col1], *M*[row][col2]**v*[col2], ...)>  
        output: <row, *M*[row][col1]**v*[col1] + *M*[row][col2]**v*[col2] + ...>
        

#### Note
1. This example uses two MapReduce jobs, and the later takes the output of the former as the input. Note the dependency of input/output directories.
2. In the first MapReduce job, two Mapper classes are used to read multiple input data, which is a good demo for the usage of **MultipleInputs** class in package *org.apache.hadoop.mapreduce.lib.input*.
