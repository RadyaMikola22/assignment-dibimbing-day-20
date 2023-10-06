"""
Script ini dirancang untuk mengambil data tentang berita properti yang ada pada laman detik.com. Data yang diambil berupa judul, 
tipe, dan waktu terbit beritanya. Data mentah tersebut bisa diolah untuk mencari tahu judul berita mana saja yang tipe beritanya
merupakan tips dan panduan atau lainnya. selain itu, data tersebut bisa dilihat berapa total berita yang terbit dalam setiap
harinya.

Adapaun script ini menggunakan selenium untuk bisa mengambil data pada web yang memiliki banyak halaman.
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from helpers import init
from sqlalchemy import create_engine
import pandas as pd
import pandas as pd
import psycopg2

# Set path ke Chromedriver
driver_path = 'C:/webdrivers/chromedriver.exe'  # Sesuaikan dengan path Chromedriver di komputer Anda

# URL path webnya
path_to_visit = "https://www.detik.com/properti/indeks"

# List untuk menyimpan data
judul_list = []
tipe_list = []
waktu_list = []

# Inisiasialisasi web driver
driver = init(
    driver_path=driver_path,
    headless=False,
    detach=True
)

# Inisialisasi WebDriverWait dengan timeout 10 detik
wait = WebDriverWait(driver, 10)

# Fungsi untuk mengambil data dari halaman
def scrape_page(path_to_visit):
    driver.get(path_to_visit)

    try:
        # Menunggu hingga elemen-elemen muncul
        berita_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@class="list-content__item"]')))

        for berita in berita_elements:
            judul_berita_tag = berita.find_element(By.XPATH, './/h3[@class="media__title"]')
            tipe_berita_tag = berita.find_element(By.XPATH, './/h2[@class="media__subtitle"]')
            waktu_berita_tag = berita.find_element(By.XPATH, './/div[@class="media__date"]')

            judul_list.append(judul_berita_tag.text)
            tipe_list.append(tipe_berita_tag.text)
            waktu_list.append(waktu_berita_tag.text)

    except Exception as e:
        print(f"Kesalahan: {e}")

# Mengambil data dari halaman pertama
scrape_page(path_to_visit)

# Mengambil data dari halaman-halaman berikutnya, ini saya batasin sampai 5 halaman aja
page_number = 2
while page_number <= 5:
    next_url = f"https://www.detik.com/properti/indeks/{page_number}"
    try:
        scrape_page(next_url)
        page_number += 1
    except Exception as e:
        print(f"Error: {e}")
        break

# Mengakhiri sesi WebDriver
driver.quit()

# Membuat DataFrame untuk di export ke csv dan ke postgresql
df = pd.DataFrame({
    "Judul": judul_list,
    "Tipe": tipe_list,
    "Waktu Terbit": waktu_list
})

# Menyimpan ke file CSV
df.to_csv("Output CSV/full_berita_properti.csv", index=False, sep=';')

print("Data berhasil diambil dan disimpan ke full_berita_properti.csv.")

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
table_name = "properti_full"

# Membuat mesin SQLAlchemy untuk menulis data ke PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}')

# Menulis DataFrame ke tabel PostgreSQL
df.to_sql(table_name, engine, if_exists="replace", index=False)

# Menutup koneksi
conn.close()

print("Data berhasil ditulis ke PostgreSQL.")
