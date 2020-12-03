Step to do :

**INSTAL PYTHON**

1. Cek python version

2. 
install python, untuk aplikasi ini dikembangkan di python-3.7.2

lalu install dependency yang dipakai dari requirement.txt

---------------------------------------------------------------

jika ingin menjalakan aplikasi, masuk ke dir tempat file berada (sinkronisasi.py)

lalu run dengan command => python sinkronisasi.py

run melalui comand prompt

-----------------------------------------------------------------
untuk pengaksesan database (mongo atlas)
ada pada file pytho yang dijalankan, di variable 'warehouse' dan 'backend'
koneksi via string connection, jadi kalau ingin mengubah value 2 variable tsb

untuk akses database lokal tinggal mengganti
client = MongoClient(port='nomor port mongo')
