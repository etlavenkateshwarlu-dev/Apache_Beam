import apache_beam as beam
SOURCE_FILE_PATH_CSV="C:\\Users\\91991\\Documents\\VT\\Learnnings\\GCP_Cloud Data Engeer\\ApacheBeamEnv\\source\\employee_1.csv"
TARGET_FOLDER="C:\\Users\\91991\\Documents\\VT\\Learnnings\\GCP_Cloud Data Engeer\\ApacheBeamEnv\\target\\employee_1.csv"

#COuntry position is countr 9 and dept is 8

def split_function(elements):
    return elements.split(',')

def filterCountryData(elements):
    return elements[8]=='US'

def groupByDept(elements):
    return (elements[7],1)

pipeline1=beam.Pipeline()
pcolletion=(
        pipeline1
        |beam.io.ReadFromText(SOURCE_FILE_PATH_CSV,skip_header_lines=1)
        |beam.Map(split_function)
        |beam.Filter(filterCountryData)
        |beam.Map(groupByDept)
        |beam.CombinePerKey(sum)
        |beam.io.WriteToText(TARGET_FOLDER)
)
pipeline1.run()
