from functools import partial

import apache_beam as beam

SOURCE_FILE_PATH_CSV="C:\\Users\\91991\\Documents\\VT\\Learnnings\\GCP_Cloud Data Engeer\\ApacheBeamEnv\\source\\employee_1.csv"
US_TARGET_FOLDER="C:\\Users\\91991\\Documents\\VT\\Learnnings\\GCP_Cloud Data Engeer\\ApacheBeamEnv\\target\\us\\employee_1.csv"
IN_TARGET_FOLDER="C:\\Users\\91991\\Documents\\VT\\Learnnings\\GCP_Cloud Data Engeer\\ApacheBeamEnv\\target\\in\\employee_1.csv"
IN_US_TARGET_FOLDER="C:\\Users\\91991\\Documents\\VT\\Learnnings\\GCP_Cloud Data Engeer\\ApacheBeamEnv\\target\\in_us_merge\\employee_1.csv"
#COuntry position is countr 9 and dept is 8

def split_function(elements):
    return elements.split(',')

def filterCountryData(elements,country):
    return elements[8]==country

def groupByDept(elements):
    return (elements[7],1)

with beam.Pipeline() as p1:
    readAndSplitColl=(
        p1
        | 'read file data' >> beam.io.ReadFromText(SOURCE_FILE_PATH_CSV,skip_header_lines=1)
        | 'split data ' >> beam.Map(split_function)
    )
    usCollection=(
        readAndSplitColl
        | 'us filter ' >> beam.Filter(lambda line: line[8]=='US')
        | 'appending values ' >> beam.Map(groupByDept)
        | 'suming the deptgroup ' >> beam.CombinePerKey(sum)
        #| 'write data to us file1 ' >> beam.io.WriteToText(US_TARGET_FOLDER)
    )
    inCollection=(
        readAndSplitColl
        | 'in filter ' >> beam.Filter(lambda line: line[8] =='IN')
        | 'in append values' >> beam.Map(groupByDept)
        | 'in suming the grouped Data' >> beam.CombinePerKey(sum)
        #| 'write IN data to file2' >> beam.io.WriteToText(IN_TARGET_FOLDER)
    )
    finalCollection=(
        (usCollection,inCollection)
        |beam.Flatten()
        |beam.io.WriteToText(IN_US_TARGET_FOLDER)
    )