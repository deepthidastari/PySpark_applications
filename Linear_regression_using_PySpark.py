#Importing libraries for machine learning algorithms
From pyspark.ml.regresion import LinearRegression

#Importing data in Spark 
all_data = spark.read.csv(‘<filename>’, inferSchema = True, header = True)
all_data.printSchema()

#Converting categorical variables to numerical:
From pyspark.ml.feature import StringIndexer
Indexer = StringIndexer(inputCol = ‘<categoricalColName>’, outputCol = ‘<nameForConvertedCol>’)
Indexed = indexer.fit(all_data).transform(all_data)   

#Creating a vector of all the input columns, because that is the format Spark expects for the machine learning libraries to work
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols = [inputCol1, inputCol2, inputCol3, inputCol4 ],outputCol = ‘features’)  

#The output variable will have all the columns in the data set plus an additional features column, which is a vector of all
#the inputColumns we gave
output = assembler.transform(Indexed)
final_data = output.select(‘features’, ‘dependentVariableName’)

#Splitting the data into training and test sets
train_data, test_data = final_data.randomSplit([0.7,0.3])
train_data.show()
test_data.describe.show()

#Building the linear regression model with input as 'features'
model = LinearRegression(featuresCol = ‘features’, labelCol = ‘<outputColName>’, predictionCol = ‘prediction’)
lrModel = lr.fit(train_data)

#Evaluate how our model performed on test data
test_results = lrModel.evaluate(test_data)  
test_resulsts.residuals.show()
test_results.rootMeanSquaredError

#Model perfomance parameter : R-square
test_results.r2

#Check what the predictions will be on data that doesn’t have a label value
unlabeled_data = test_data.select(‘features’)  
predictions = lrModel.transform(unlabeled_data)
predictions.show()
