from pyspark.sql import SparkSession

input_transaksi_path = '/home/nandaap/transaksi.csv'
input_referensi_path = '/home/nandaap/referensi.csv'
output_path = '/home/nandaap/output'

def main(input_transaksi_path, input_referensi_path, output_path):
    spark = SparkSession.builder.appName("PostgresToSpark").getOrCreate()
    
    # Membaca data dari tabel transaksi
    df_data_transaksi = spark.read.csv(input_transaksi_path, header=True, inferSchema=True)
    
    # Membaca data dari tabel referensi
    df_data_referensi = spark.read.csv(input_referensi_path, header=True, inferSchema=True)
    
    # Lakukan pemrosesan data di sini, misalnya:
    # Gabungkan data dari tabel transaksi dengan tabel referensi
    df_joined = df_data_transaksi.join(df_data_referensi, df_data_transaksi['data_referensi_id'] == df_data_referensi['id'], how='left')
    
    # Simpan hasil pemrosesan ke database postgresql
    df_joined.write.csv(output_path + '/data_bank_tr.csv', header=True, mode='overwrite')
                
    spark.stop()

if __name__ == "_main_":
    import sys
    main(sys.argv[1], sys.argv[2], sys.argv[3])