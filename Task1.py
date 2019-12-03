import pyspark
from pyspark import SparkContext
from pyspark.sql.types import IntegerType, DoubleType, DateType, StringType
import re
from datetime import datetime

sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("Project-Task1") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

filePath = '/user/mw4086/NYCOpenData-Sample'

fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
fileStatusList = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(filePath))
fileNameList = [fileStatus.getPath().getName() for fileStatus in fileStatusList]

def loadData(fileName):
    dataFrame = spark.read.csv(filePath + '/' + fileName, header='true', inferSchema='true', sep='\t')
    file = sc.textFile(filePath + '/' + fileName)
    header = file.first().split('\t')
    data = file.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda x: x[0]).map(lambda x: x.split('\t'))
    return header, data, dataFrame

def defineDataType(value):
    integer_expr = r'^[+-]?[1-9]\d*$'
    real_expr = r'^[+-]?(\d+.\d*)|(\d*.\d+)$'
    if (re.match(integer_expr, value)):
        return 'INTEGER(LONG)'
    if (re.match(real_expr, value)):
        return 'REAL'
    if (datetime.strptime(value, '%Y-%m-%d') or datetime.strptime(value, '%Y/%m/%d')):
        return 'DATE/TIME'
    return 'TEXT'

def processColumnValueWithType(x):
    if x[0] == 'INTEGER(LONG)':
        return (x[0], x[1], int(x[1]))
    elif x[0] == 'INTEGER(LONG)':
        return (x[0], x[1], float(x[1]))
    elif x[0] == 'DATE/TIME':
        return (x[0], x[1], datetime(x[1]))
    else:
        return (x[0], x[1], len(x[1]))

def processColumn(header, data, dataFrame):
    columnNumber = len(header)
    tableKeyList = []
    for i in range(columnNumber):
        columnName = header[i]
        columnValue = data.map(lambda x: x[i])
        nonEmptyCellNumber = columnValue.filter(lambda x: x != '').count()
        emptyCellNumber = columnValue.filter(lambda x: x == '').count()
        valueNumber = columnValue.count()
        distinctValueNumber = columnValue.distinct().count()
        if valueNumber == distinctValueNumber:
            tableKeyList.append(columnName)
        top5FrequentValues = columnValue.map(lambda x: (x, 1)).reduceByKey(lambda x1, x2: x1 + x2).takeOrdered(5, key=lambda x: -x[1])

        dfDataType = dataFrame.schema[i].dataType
        typeMap = {IntegerType: 'INTEGER(LONG)',
                   DoubleType: 'REAL',
                   DateType: 'DATE/TIME'}
        if (type(dfDataType) != StringType):
            dataType = typeMap[type(dfDataType)]
            typeList = [dataType]
            columnWithType = columnValue.map(lambda x: (dataType, x))
        else:
            columnWithType = columnValue.map(lambda x: (defineDataType(x), x))
            typeList = columnWithType.keys().distinct().collect()
        columnProcessValue = columnWithType.map(lambda x: processColumnValueWithType(x))
        typeCount = columnProcessValue.countByKey()
        for columnType in typeList:
            typeRow = columnProcessValue.filter(lambda x: x[0] == columnType)
            typeValue = typeRow.map(lambda x: x[2])
            typeInfo = [typeValue.max(), typeValue.min(), typeValue.mean(), typeValue.stdev()]
            if columnType == 'TEXT':
                top5LongestText = typeRow.takeOrdered(5, key=lambda x: -x[2]).collect()
                top5ShortestText = typeRow.takeOrdered(5, key=lambda x: x[2]).collect()
                avgLength = typeValue.sum() / typeValue.count()


for fileName in fileNameList:
    header, data, dataFrame = loadData(fileName)
    processColumn(header, data, dataFrame)



