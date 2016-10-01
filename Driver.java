
public class Driver {

	public static void main(String[] args) throws Exception {
		DataDivideByUser dataDivideByUser = new DataDivideByUser();
		CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
		Multiplication multiplcation = new Multiplication();
		RecommenderListGenerator recommenderListGenerator = new RecommenderListGenerator();
		
		String[] path1 = new String[] {args[0], args[1]};
		String[] path2 = new String[] {args[1], args[2]};
		String[] path3 = new String[] {args[5], args[0], args[4]};
		String[] path4 = new String[] {args[6], args[7], args[4], args[8]};
		
		dataDivideByUser.main(path1);
		coOccurrenceMatrixGenerator.main(path2);
		multiplcation.main(path3);
		recommenderListGenerator.main(path4);
	}
}
