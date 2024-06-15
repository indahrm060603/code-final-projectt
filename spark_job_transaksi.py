from pyspark.sql import SparkSession

input_transaksi_path = '/home/nandaap/transaksi.csv'
output_path = '/home/nandaap/output'

def main(input_transaksi_path, output_path):
    spark = SparkSession.builder.appName("PostgresToSpark").getOrCreate()
    
    # Membaca data dari tabel transaksi
    df_transaksi = spark.read.csv(input_transaksi_path, header=True, inferSchema=True)
    
    # Lakukan pemrosesan data di sini, misalnya: filter,agregasi,dll
    # Gabungkan data dari tabel transaksi dengan tabel referensi
    df_filtered = df_transaksi.filter(df_transaksi['amount'] > 100)
    
    # Simpan hasil pemrosesan ke database postgresql
    df_filtered.write \
        .format('jdbc') \
            .option('output') \
            .option('postgresnandaconn') \
            .option('nandaap10') \
            .mode('overwrite') \
            .save()
                
    spark.stop()

if __name__ == "_main_":
    import sys
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])