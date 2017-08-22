## Matrix-Vector Multiplication

#### Problem
Calculate the product of two matrixs *M1* and *M2* (both assumed sparse)

Here we demonstrate using a toy example  
![toy_example](./toy_example.png)


#### Workflow - 2 MapReduce jobs
1. CellMultiplication: Mapper1 + Mapper2 ---> Reducer  
    - Mapper1: read the non-zero elements from matrix1 file (i1 j1 *M1*[i1][j1])  
        input: <offset, line>  
        output: <j1, i1=*M1*[i1][j1]>
    - Mapper2: read the non-zero elements from matrix2 file (i2 j2 *M2*[i2][j2])  
        input: <offset, line>  
        output: <i2, j2:*M2*[i2][j2]>
    - Reducer: multiply a matrix1 cell with the corresponding matrix2 cell  
        input: <k, (i1=*M1*[i1][k], i2=*M1*[i2][k], ..., j1=*M2*[k][j1], j2=*M2*[k][j2], ...)>  
        output: <i:j, *M1*[i][k]**M2*[k][j]>

2. CellSum: Mapper ---> Reducer
    - Mapper: read the intermediate results of cell multiplication 
        input: <offset, line>  
        output: <i:j, *M1*[i][k]**M2*[k][j]> 
    
    - Reducer: sum up all the cell product to the final value for each vector row  
        input: <i:j, (*M1*[i][k1]**M2*[k1][j], *M1*[i][k2]**M2*[k2][j], ...)>  
        output: <i \t j, *M1*[i][k1]**M2*[k1][j] + *M1*[i][k2]**M2*[k2][j] + ...>
        

#### Note
1. This example is a follow-up implementation of the Matrix-Vector Multiplication problem.
2. This example uses two MapReduce jobs, and the later takes the output of the former as the input. Note the dependency of input/output directories.
3. In the first MapReduce job, two Mapper classes are used to read multiple input data, which is a good demo for the usage of **MultipleInputs** class in package *org.apache.hadoop.mapreduce.lib.input*.