import apache_beam as beam
SOURCE_FILE_PATH_CSV="C:\\Users\\91991\\Documents\\VT\\Learnnings\\GCP_Cloud Data Engeer\\ApacheBeamEnv\\source\\employee_1.csv"
TARGET_FOLDER="C:\\Users\\91991\\Documents\\VT\\Learnnings\\GCP_Cloud Data Engeer\\ApacheBeamEnv\\target\\employee_1.csv"

#COuntry position is countr 9 and dept is 8

def split_function(elements):
    return elements.split(',')

def filterCountryData(elements):
    return elements[8]=='IN'

def groupByDept(elements):
    return (elements[7],1)

with beam.Pipeline() as p1:
    pcolletion=(
        p1
        | 'Read data from text file' >> beam.io.ReadFromText(SOURCE_FILE_PATH_CSV,skip_header_lines=1)
        | 'spliting data as , ' >> beam.Map(split_function)
        | 'filter data for us record '>>beam.Filter(filterCountryData)
        | 'appending 1 for every record '>>beam.Map(groupByDept)
        | 'group and suming ' >> beam.CombinePerKey(sum)
        | 'write result data to file ' >> beam.io.WriteToText(TARGET_FOLDER)
)