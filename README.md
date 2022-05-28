Create VENV : python3.9-m venv pyspark3 // Created virtual env with name pyspark3
Activate VENV : source ./pyspark-3/bin/activate

link data source text : https://www.gutenberg.org/cache/epub/100/pg100.txt

Learn:
- Data Engginering
- Data Processing
- Distributed DataProcessing

Dataenginer : Bagaimana mengektract data supaya memilikivalue dan bisadimanfaatkan oleh user. Sehingga semuakeputusan user berdasarkan data. 


Perbedaan DDP dan DP: Membagidata besar menjadi beberapabagian untuk running atauproses datadidalamnode yanglebih kecil

Kelebihan DDP
- Untukmemproses data besar memekanwaktu yang cepat
Data akan direplika agar tidak ada data yg hilang
Kalau server down, tidak mempengaruhikeseluruhan proses

DP in tokpedia
- Big Query
- Pythonon
- Spark
- Beam


Apache Spark ? Unified analytics engginer ? Tools yang digunakanuntuk analyssis seperti mengambil data,extarct data maupunvisualisasi data. Kusus untuk datayang sangat besar. Bisa digunakan Jva,scala,python, R.

Why Spark ? 
- Dtaa ynag besar dengan levelterrabite bisa berjalandengan speed, canbe faster 100x than hadoopfor large scale data processing
- Easy of use
- A unified enggine : Sql queries, streaming data, machine learning, and

Arcitecture Spark Cluster

Master Spark memiliki beberapa worker spark





Referensi :
https://stackoverflow.com/questions/69594088/error-when-creating-venv-error-command-im-ensurepip-upgrade-def
https://stackoverflow.com/questions/14604699/how-to-activate-virtualenv-in-linux
https://stackoverflow.com/questions/42697026/install-google-cloud-components-error-from-gcloud-command