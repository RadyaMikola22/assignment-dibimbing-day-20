"""
Script ini dirancang untuk mengambil data tentang berita properti yang ada pada laman detik.com. Data yang diambil berupa judul, 
tipe, dan waktu terbit beritanya. Data mentah tersebut bisa diolah untuk mencari tahu judul berita mana saja yang tipe beritanya
merupaka tips dan panduan atau lainnya. selain itu, data tersebut bisa dilihat berapa total berita yang terbit dalam setiap
harinya.

Adapaun script ini menggunakan beautifulsoup.
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Mengambil html pada web yang dituju
url = "https://www.detik.com/properti/"
page = requests.get(url)
soup = BeautifulSoup(page.content, 'html.parser')

# Menemukan semua kontainer berita
kontainer_berita = soup.find_all("article", class_="ph_newsfeed_d article_inview list-content__item")

# Inisialisasi list untuk judul dan tipe berita
judul_list = []
tipe_list = []
waktu_list = []

# Loop untuk mengumpulkan data judul, tipe, dan waktu terbit berita
for berita in kontainer_berita:
    judul_berita_tag = berita.find("h3", class_="media__title")
    tipe_berita_tag = berita.find("h2", class_="media__subtitle")
    waktu_berita_tag = berita.find("div", class_="media__date")

    judul_list.append(judul_berita_tag.text)
    tipe_list.append(tipe_berita_tag.text)
    waktu_list.append(waktu_berita_tag.text)

# Membuat DataFrame untuk di export ke csv dan ke postgresql
df = pd.DataFrame({
    "Judul": judul_list,
    "Tipe": tipe_list,
    "Waktu Terbit": waktu_list
})

# Menghapus duplikat berdasarkan judul berita
df = df.drop_duplicates(subset="Judul")

# Menyusun ulang indeks DataFrame
df = df.reset_index(drop=True)

# Membersihkan teks judul dan tipe
df['Judul'] = df['Judul'].str.strip()  # Menghapus spasi awal dan akhir
df['Tipe'] = df['Tipe'].str.strip()    # Menghapus spasi awal dan akhir
df['Waktu Terbit'] = df['Waktu Terbit'].str.strip()  # Menghapus spasi awal dan akhir

# Menyimpan DataFrame ke dalam file CSV
df.to_csv("Output CSV/berita_properti.csv", index=False, sep=';')

print("Data berhasil diambil dan disimpan ke berita_properti.csv.")

# Pengaturan koneksi ke PostgreSQL
db_config = {
    "host": "localhost",
    "port": "5433",
    "database": "scrapping",
    "user": "postgres",
    "password": "<password>"  # Ini saya buat begini agar password saya tidak tersebar
}

# Membuat koneksi ke database PostgreSQL
conn = psycopg2.connect(**db_config)

# Nama tabelnya
table_name = "properti"

# Membuat mesin SQLAlchemy untuk menulis data ke PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}')

# Menulis DataFrame ke tabel PostgreSQL
df.to_sql(table_name, engine, if_exists="replace", index=False)

# Menutup koneksi
conn.close()

print("Data berhasil ditulis ke PostgreSQL.")
