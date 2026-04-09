
from extract_url import *
import time
from store_data_database import *


file_name = "Amul Gold Milk Price in India - Buy Amul Gold Milk online at Flipkart.com.html"

def main():
    print("main function")

    ## this block comment for checking
    # create table 
    create_table_merck_url()
    print("first table created")

    #extract url
    extract_data_list = fetch_urls()

    # insert data
    merck_url_insert(list_data=extract_data_list)
    print("inserted data")

    # fetch merck_product_url 
    # print("fetch data")

    fetch_merck_url_list = fetch_merck_url_table_data()
    print("second table created")

    # create product_url table
    create_table_product_url()

    # delete child recode if parent id is pending.
    delete_crupt_child_recode()

    print("fetch_merck_url_list type : ", type(fetch_merck_url_list))



    # child_product_url(list_data=fetch_merck_url_list)

    # list_num = fetch_merck_url_list[:1]
    # print("list_num : ", fetch_merck_url_list)
    print("2 list_num : ", len(fetch_merck_url_list))
    
    worker(list_data=fetch_merck_url_list)
    
    



    # create_table()
    # print("table and db create")
    # html_content = read_html_content(file_name)
    # product_list = extract_data_from_html(html_content)
    # insert_data_in_table(list_data=product_list)


if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print("time different  : ", end - start)





