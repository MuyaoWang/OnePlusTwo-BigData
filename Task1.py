import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, DateType, StringType, LongType, TimestampType, BooleanType
import re
from datetime import datetime
import json
import dateutil.parser

sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("Project-Task1") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

filePath = '/user/mw4086/NYCOpenData-Sample'
# filePath = '/user/hm74/NYCOpenData'

fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
fileStatusList = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(filePath))
fileNameList = [fileStatus.getPath().getName() for fileStatus in fileStatusList]

def loadData(fileName):
    dataFrame = spark.read.csv(filePath + '/' + fileName, header='true', inferSchema='true', sep='\t')
    file = sc.textFile(filePath + '/' + fileName)
    header = file.first().split('\t')
    data = file.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda x: x[0]).map(lambda x: x.split('\t'))
    return header, data, dataFrame

def addDataType(dataType, x):
    integer_expr = r'^[+-]?[1-9]\d*$'
    real_expr1 = r'^[+-]?\d+\.\d*$'
    real_expr2 = r'^[+-]?\d*\.\d+$'
    if x.strip() == '':
        return ('TEXT', x, len(x))
    if dataType == 'INTEGER(LONG)':
        return (dataType, x, int(x))
    elif dataType == 'REAL':
        return (dataType, x, float(x))
    elif dataType == 'DATE/TIME':
        return (dataType, x[1], datetime(x[1]))
    else:
        if (re.match(integer_expr, x)):
            return ('INTEGER(LONG)', x, int(x))
        elif (re.match(real_expr1, x) or re.match(real_expr2, x)):
            return ('REAL', x, float(x))
        else:
            try:
                new_x = dateutil.parser.parse(x, default = datetime(1900, 1, 1, 0, 0, 0, 000000))
                return ('DATE/TIME', x, new_x)
            except ValueError:
                pass
        try:
            new_x = datetime.strptime(x, '%I%MA')
            return ('DATE/TIME', x, new_x)
        except ValueError:
            pass
        try:
            new_x = datetime.strptime(x, '%I%MP')
            return ('DATE/TIME', x, new_x)
        except ValueError:
            pass
        return ('TEXT', x, len(x))

def processColumn(header, data, dataFrame):
    columnNumber = len(header)
    columnList = []
    tableKeyList = []
    for i in range(columnNumber):
        columnDict = {}
        columnName = header[i]
        columnDict['column_name'] = columnName
        columnValue = data.map(lambda x: x[i])
        nonEmptyCellNumber = columnValue.filter(lambda x: x.strip() != '').count()
        columnDict['number_non_empty_cells'] = nonEmptyCellNumber
        emptyCellNumber = columnValue.filter(lambda x: x.strip() == '').count()
        columnDict['number_empty_cells'] = emptyCellNumber
        valueNumber = columnValue.count()
        distinctValueNumber = columnValue.distinct().count()
        columnDict['number_distinct_values'] = distinctValueNumber
        if valueNumber == distinctValueNumber:
            tableKeyList.append(columnName)
        top5FrequentValues = columnValue.map(lambda x: (x, 1)).reduceByKey(lambda x1, x2: x1 + x2).takeOrdered(5, key=lambda x: -x[1])
        columnDict['frequent_values'] = list(map(lambda x: x[0], top5FrequentValues))
        dfDataType = dataFrame.schema[i].dataType
        typeMap = {IntegerType: 'INTEGER(LONG)',
                   LongType: 'INTEGER(LONG)',
                   DoubleType: 'REAL',
                   DateType: 'DATE/TIME',
                   TimestampType: 'DATE/TIME',
                   BooleanType: 'TEXT',
                   StringType: 'TEXT'}
        dataType = typeMap[type(dfDataType)]
        columnWithType = columnValue.map(lambda x: addDataType(dataType, x))
        typeList = columnWithType.keys().distinct().collect()
        typeCount = columnWithType.countByKey()
        dataTypeList = []
        for columnType in typeList:
            dataTypeDict = {}
            dataTypeDict['type'] = columnType
            dataTypeDict['count'] = typeCount[columnType]
            typeRow = columnWithType.filter(lambda x: x[0] == columnType)
            typeValue = typeRow.map(lambda x: x[2])
            if columnType == 'TEXT':
                top5LongestText = typeRow.takeOrdered(5, key=lambda x: -x[2])
                top5ShortestText = typeRow.takeOrdered(5, key=lambda x: x[2])
                avgLength = typeValue.sum() / typeValue.count()
                dataTypeDict['shortest_values'] = list(map(lambda x: x[1], top5ShortestText))
                dataTypeDict['longest_values'] = list(map(lambda x: x[1], top5LongestText))
                dataTypeDict['average_length'] = avgLength
            else:
                dataTypeDict['max_value'] = typeValue.max()
                dataTypeDict['min_value'] = typeValue.min()
                if columnType == 'INTEGER(LONG)' or columnType == 'REAL':
                    dataTypeDict['mean'] = typeValue.mean()
                    dataTypeDict['stddev'] = typeValue.stdev()
            dataTypeList.append(dataTypeDict)
        columnDict['data_types'] = dataTypeList
        columnList.append(columnDict)
    return columnList, tableKeyList

def writeToJson(fileName, dataDict):
    jsondict = json.dumps(dataDict)
    f = open(fileName[0:9]+'.json', 'w')
    f.write(jsondict)
    f.close()

'''
datetimef = open('datetimesample.txt', 'w')
for k, fileName in enumerate(fileNameList):
    dataFrame = spark.read.csv(filePath + '/' + fileName, header='true', inferSchema='true', sep='\t')
    schema = dataFrame.schema
    length = len(schema)
    for i in range(length):
        columnType = schema[i].dataType
        columnName = dataFrame.columns[i]
        if columnType == DateType or columnType == TimestampType or 'date' in columnName.lower() or 'time' in columnName.lower():
            dataList = dataFrame.select(columnName).take(5)
            for d in dataList:
                datetimef.write('%s\n' % d[columnName])
            print('-------------------------------------')
    print('++++++++++++++++++++++++++++++++++++++',k)
datetimef.close()
'''
failFileName = []
for fileName in fileNameList:
    try:
        dataDict = {}
        dataDict['dataset_name'] = fileName
        header, data, dataFrame = loadData(fileName)
        columnList, tableKeyList = processColumn(header, data, dataFrame)
        dataDict['columns'] = columnList
        dataDict['key_column_candidates'] = tableKeyList
        writeToJson(fileName, dataDict)
        print(fileName)
    except:
        failFileName.append(fileName)
if len(failFileName) > 0:
    failF = open('failFile.txt', 'w')
    for fileN in failFileName:
        failF.write('%s\n' % fileN)
    failF.close()

sc.stop()