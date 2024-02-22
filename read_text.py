from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time

# Bắt đầu tính thời gian
start_time = time.time()
hdfs_path = "hdfs://localhost:9000/student_inf"
# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("ReadTextFile") \
    .master("local[1]") \
    .getOrCreate()

# Định nghĩa schema cho DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", StringType(), True),
    StructField("year", StringType(), True),
    StructField("description", StringType(), True),
    StructField("isNew", StringType(), True)
])

def process_line(line):
    # Lấy thông tin từng trường
    name = line[:4]
    age = line[5:7]
    year = line[8:13]
    description = line[13:]
    isNew = False
    if int(year) > 2022 :
        isNew = True        
    return name,age,year,description,isNew

# Đường dẫn tới file
file_path = 'D:\Tek Experts\TPBank\Code\ReadText\sample_small.txt'

# Đọc dữ liệu từ file và chuyển đổi thành DataFrame
text_rdd = spark.sparkContext.textFile(file_path)

data_rdd = text_rdd.map(process_line)
df = spark.createDataFrame(data_rdd, schema)

df.write.parquet(hdfs_path)
# In ra nội dung của DataFrame
df.show(truncate=False)



# Tiếp tục các bước tiếp theo như định nghĩa schema, đọc dữ liệu, và xử lý DataFrame

# Kết thúc tính thời gian
end_time = time.time()

# In ra thời gian chạy của job
print("Thời gian chạy của job: {} giây".format((end_time - start_time)/60))