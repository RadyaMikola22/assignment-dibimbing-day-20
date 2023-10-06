# Tugas Day 20 - Web Scrapping
- Repository ini merupakan tugas dari dibimbing Day 20 - Web Scrapping
- Terdapat 2 script yang saya buat dengan nama berita_properti_beautifulsoup (menggunakan beautiful soup) dan fullberita_properti_selenium (menggunakan selenium)
- Terdapat script helper.py untuk menginisiasikan chromerdrivernya

# Setup
1. Create virtual environment using `pyenv virtualenv <python-version> <env-name>` or `python3 -m venv venv` (the simplest way).
2. Install the required libs with:
```
pip install -r requirements.txt
```
3. Look for ChromeDriver https://chromedriver.chromium.org/getting-started. Check your browser's version first then download the driver with the **EXACT SAME VERSION** to your browser.

# For MacOS Users
If you faced `Chromium is damaged and can't be opened`. Try to exec this command
```
sudo xattr -cr /Applications/Chromium.app
```

If you faced `chromedriver” can’t be opened because Apple cannot check it for malicious software.` error. Try to exec this command
```
sudo xattr -d com.apple.quarantine chromedriver_mac64/chromedriver
```
