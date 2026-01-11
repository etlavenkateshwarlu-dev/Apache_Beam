import  apache_beam as beam
#C:\Users\91991\Documents\VT\Learnnings\GCP_Cloud Data Engeer\ApacheBeamEnv
SOURCE_FILE_PATH_CSV="C:\\Users\\91991\\Documents\\VT\\Learnnings\\GCP_Cloud Data Engeer\\ApacheBeamEnv\\source\\employee_1.csv"
TARGET_FOLDER="C:\\Users\\91991\\Documents\\VT\\Learnnings\\GCP_Cloud Data Engeer\\ApacheBeamEnv\\target\\employee_1.csv"

with beam.Pipeline() as p1:
    pcoll=(
            p1
            |beam.io.ReadFromText(SOURCE_FILE_PATH_CSV)
            |beam.io.WriteToText(TARGET_FOLDER)
    )