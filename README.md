# Merck Product URL Scraper Using Scrapy

## 📖 Overview

This project is a Python-based web scraping solution developed using Scrapy to extract product URLs and product-related information from the Merck website. The scraper is designed to collect structured product data efficiently and demonstrate scalable web scraping practices.

The project showcases real-world experience in data extraction, URL collection, response parsing, and Scrapy-based development.

---

## 🚀 Features

* Product URL extraction
* Automated data collection
* Scrapy spider implementation
* Structured JSON output
* Error handling and logging
* Scalable scraping workflow

---

## 🛠️ Technologies Used

* Python
* Scrapy
* XPath
* JSON
* Logging

---

## 📊 Extracted Data

* Product Name
* Product URL
* Product Category
* Product Information
* Product Identifier

---

## 📁 Project Structure

```text
merck-product-url-scrapy/
│
├── scrapy.cfg
├── requirements.txt
├── README.md
│
└── merck_product_url/
    │
    ├── __init__.py
    ├── items.py
    ├── pipelines.py
    ├── settings.py
    │
    └── spiders/
        │
        ├── __init__.py
        └── merck_product_url.py
```

---

## ⚡ Installation

```bash
pip install -r requirements.txt
```

---

## ▶️ Run Spider

```bash
scrapy crawl merck_product_url
```

---

## 📂 Export Output

```bash
scrapy crawl merck_product_url -o product_urls.json
```

---

## 🎯 Learning Outcomes

* Scrapy Framework
* XPath Extraction
* URL Discovery and Collection
* JSON Data Processing
* Spider Development
* Error Handling and Logging
* Scalable Web Scraping Architecture

---

### 🔗 GitHub Profiles

💼 Professional Work:
https://github.com/vishal-kushvanshi-2508

📚 Practice Projects & Learning:
https://github.com/vishal-2508

