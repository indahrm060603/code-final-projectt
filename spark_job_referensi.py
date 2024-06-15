from pyspark.sql import SparkSession

input_referensi_path = '/home/nandaap/referensi.csv'
output_path = '/home/nandaap/output'

def main(input_referensi_path, output_path):
    spark = SparkSession.builder.appName("PostgresToSpark").getOrCreate()
    
    # Membaca data dari tabel referensi
    df_referensi = spark.read.csv(input_referensi_path, header=True, inferSchema=True)
    
    # Lakukan pemrosesan data di sini, misalnya: filter,agregasi,dll
    # Gabungkan data dari tabel referensi dengan tabel referensi
    df_filtered = df_referensi.filter(df_referensi['amount'] > 100)
    
    # Simpan hasil pemrosesan ke database postgresql
    df_filtered.write \
        .format('jdbc') \
            .option('tabel_data_referensi') \
            .option('postgresnandaconn') \
            .option('nandaap10') \
            .mode('overwrite') \
            .save()
                
    spark.stop()

if __name__ == "_main_":
    import sys
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])