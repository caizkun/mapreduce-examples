public class Driver {

    public static void main(String[] args) throws Exception {
        CellMultiplication multiplication = new CellMultiplication();
        CellSum sum = new CellSum();

        // parse args
        String matrixInputPath = args[0];
        String vectorInputPath = args[1];
        String subSumOutputPath = args[2];
        String sumOutputpath = args[3];

        String[] cellMultiplicationArgs = {matrixInputPath, vectorInputPath, subSumOutputPath};
        multiplication.main(cellMultiplicationArgs);

        String[] cellSumArgs = {subSumOutputPath, sumOutputpath};
        sum.main(cellSumArgs);
    }
}
