#Creating a base DataFrame and performing operations 

wordsDF = sqlContext.createDataFrame([('the',), ('world',), ('is',), ('is',), ('the', )], ['word'])
wordsDF.show()
print type(wordsDF)
wordsDF.printSchema()

#Using dataframe function to add an 's'

from pyspark.sql.functions import lit, concat
pluralDF = wordsDF.select(concat(wordsDF.word,lit("s")).alias('word'))
pluralDF.show()

#Find length of each word

from pyspark.sql.functions import length
pluralLengthsDF = pluralDF.select(length('word'))
pluralLengthsDF.show()

#Counting with SparkSQL

wordCountsDF = (wordsDF.groupBy('word').count())
wordCountsDF.show()

#Finding unique words and mean value

uniqueWordsCount = wordCountsDF.count()
print uniqueWordsCount

#Means of groups using DataFrames 

averageCount = (wordCountsDF.groupBy().avg('count').first()[0])
print averageCount

#The wordCount function

def wordCount(wordListDF):
    """Creates a DataFrame with word counts.

    Args:
        wordListDF (DataFrame of str): A DataFrame consisting of one string column called 'word'.

    Returns:
        DataFrame of (str, int): A DataFrame containing 'word' and 'count' columns.
    """
    wordsDF= wordListDF.groupBy('word').count()
    return wordsDF  

wordCount(wordsDF).show()

#Handling capitalization and punctuation

from pyspark.sql.functions import regexp_replace, trim, col, lower
def removePunctuation(column):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        column (Column): A Column containing a sentence.

    Returns:
        Column: A Column named 'sentence' with clean-up operations applied.
    """
    column = regexp_replace(column,'[^a-zA-Z||\s||^0-9]',"").alias('column')
    column = trim(lower(column)).alias('column')
    return column

sentenceDF = sqlContext.createDataFrame([('Hey, there!',),
                                         (' No under_score!',),
                                         (' *      Remove punctuation then spaces  * ',)], ['sentence'])
sentenceDF.show(truncate=False)
(sentenceDF
 .select(removePunctuation(col('sentence')))
 .show(truncate=False))
 
 
#Load a text file

fileName = "<full_path_to_the_file>/<filename>.txt"
shakespeareDF = sqlContext.read.text(fileName).select(removePunctuation(col('value')))
shakespeareDF.show(15, truncate=False) 
 
#Extract words from lines

from pyspark.sql.functions import split, explode
shakeWordsDF1 = (shakespeareDF.select(split(shakespeareDF.column,' ').alias('word')))
shakeWordsDF2 = shakeWordsDF1.select(explode(shakeWordsDF1.word).alias('word'))
shakeWordsDF = shakeWordsDF2.where(length(shakeWordsDF2.word)> 0 ).alias('word')
shakeWordsDF.show()
shakeWordsDFCount = shakeWordsDF.count()
print shakeWordsDFCount

#Count the words
from pyspark.sql.functions import desc
topWordsAndCountsDF1 = wordCount(shakeWordsDF)
topWordsAndCountsDF = topWordsAndCountsDF1.orderBy(topWordsAndCountsDF1['count'].desc())
topWordsAndCountsDF.show()
