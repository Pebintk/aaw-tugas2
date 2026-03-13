# Tugas A2 - AAW

### Menjawab pertanyaan tugas

#### Demonstrasikan bagaimana event dikirim, disimpan dalam topic/queue, dan diproses oleh consumer secara real-time
> Producer : pada `producer/main.go`, sistem akan memformat data login user menjadi format JSON untuk menggunakan `json.Marshal()`. Yang kemudian akan dikirim ke Kafka sebagai message broker menggunakan `w.WriteMessages()` sesuai dengan topic kafka yang sudah di konfigurasi pada `&kafka.Writer()`

> Kafka : Kafka menerima pesan yang dikirim oleh `producer/main.go`, kemudian akan menyimpan pesan tersebut secara persistent di dalam disk sesuai dengan topic `tugas-a2`. Penyimpanan ini akan menyimpan seluruh pesan dari awal topic dibuat, dan tidak akan hilang dari disk walaupun consumer sudah membaca pesan tersebut. Untuk melihat secara langsung dari isi topic kafka, dapat juga menggunakan `kafka-console-consumer` dan memilih topic yang ingin dilihat.

> consumer : pada `consumer/main.go`, sistem akan membuka koneksi dengan kafka menggunakan `kafka.NewReader()`. Consumer akan melakukan loop terus menerus untuk membaca pesan yang masuk kedalam topics menggunakan `r.ReadMessage()`. Kemudian pesan yang diterima akan didecode menggunakan `json.Unmarshal()` sesuai dengan struct `UserEvent`, yang kemudian dapat digunakan untuk implementasi logika bisnis.


#### Lakukan pengujian dengan mengirim beberapa event dan jelaskan bagaimana mekanisme komunikasi asynchronous tersebut bekerja serta apa perbedaannya dengan komunikasi request–response biasa. 
> Asynchronous: Dari melakukan loop untuk mengirim 5 event. pada Consumer, terdapat jeda 2 detik untuk mensimulasikan proses validasi login. Karena sistem ini asinkron, Producer tidak perlu menunggu Consumer menyelesaaikan proses verifikasinya. Producer hanya bertugas menaruh pesan ke Kafka, lalu langsung lanjut mengeksekusi perintah berikutnya. Hasilnya, Producer selesai mengirim 5 data hanya dalam waktu ~5 detik, sementara Consumer tetap bekerja di background memproses antrean selama ~10 detik tanpa mengganggu kinerja Producer.

> request-response: Jika sistem ini menggunakan arsitektur Request-Response yang sinkron (misalnya REST API), Login screen (Producer) harus memanggil layanan login secara langsung dan menunggu respons dari server authentication. Jika satu proses login butuh 2 detik, maka Aplikasi Web harus menunggu 2 detik penuh sebelum bisa merespons user bahwa login berhasil dilakukan. Untuk 5 user, total waktu tunggunya menjadi 10 detik. Komunikasi Asynchronous memecahkan masalah bottleneck ini dengan memisahkan (decoupling) proses penerimaan data dari proses pengeksekusian tugas yang berat.