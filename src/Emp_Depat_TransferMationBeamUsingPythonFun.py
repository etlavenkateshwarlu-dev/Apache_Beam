import apache_beam as beam
SOURCE_FILE_PATH_CSV="C:\\Users\\91991\\Documents\\VT\\Learnnings\\GCP_Cloud Data Engeer\\ApacheBeamEnv\\source\\employee_1.csv"
TARGET_FOLDER="C:\\Users\\91991\\Documents\\VT\\Learnnings\\GCP_Cloud Data Engeer\\ApacheBeamEnv\\target\\employee_1.csv"

#COuntry position is countr 9 and dept is 8

with beam.Pipeline() as p1:
    pcoll=(
            p1
            |beam.io.ReadFromText(SOURCE_FILE_PATH_CSV,skip_header_lines=1)
            |beam.Map(lambda line: line.split(','))
            |beam.Filter(lambda record:record[8]=='IN')
            |beam.Map(lambda record:(record[7],1))
            |beam.CombinePerKey(sum)
            |beam.io.WriteToText(TARGET_FOLDER)
    )



    # lines=(
    #     p1
    #     | "read csv file " >> beam.io.ReadFromText(SOURCE_FILE_PATH_CSV)
    # )
    # splitLines=(
    #     lines
    #     | "split the line with ," >> beam.Map(lambda line: line.split(','))
    # )
    # filteredLines=(
    #     splitLines
    #     | 'filter line only with country US' >> beam.Filter(lambda line:line[9] == 'US')
    # )
    # groupByDeptColl=(
    #     splitLines
    #     |'Group  dept ' >> beam.Map(lambda line:(line[8],1))
    #     |'sum the dept count ' >> beam.CombinePerKey(sum)
    # )
    # groupByDeptColl | beam.io.WriteToCsv(TARGET_FOLDER)
    #

