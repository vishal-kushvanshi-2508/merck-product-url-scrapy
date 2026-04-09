
from curl_cffi.requests import Session
from lxml import html
import json
from urllib.parse import urljoin
from store_data_database import *
import os



url="https://www.sigmaaldrich.com/AU/en"
base_url="https://www.sigmaaldrich.com/"


# No custom headers needed — impersonate handles this
SESSION = Session(impersonate="chrome120")

def fetch_urls():
    print("-----fetch_urls------")

    urls_data = []
    res = SESSION.get(
        "https://www.sigmaaldrich.com/AU/en"
    )

    # D:\vishal_kushvanshi\scrapy\merck_product_pages\merck_main_page
    folder_path = r"D:\vishal_kushvanshi\scrapy\merck_product_pages\merck_main_page"

    # next_folder_path = os.path.join(folder_path, )
    os.makedirs(folder_path, exist_ok=True)

    # pages_path = os.path.join(next_folder_path, str(start_page_num))
    #     # print("pages_path", pages_path)

    with open(f"{folder_path}\\base_page.html.gz", "w", encoding='utf-8') as f:
        f.write(res.text)

    tree = html.fromstring(res.text)
    data = tree.xpath('//script[@id="__NEXT_DATA__"]/text()')
    # print("main function", tree)
    # with open ("merch1.html", "w" , encoding='utf-8') as f:
    #     f.write(res.text)

    if data:
        json_data = json.loads(data[0])

        nav = (
            json_data
            .get("props", {})
            .get("apolloState", {})
            .get("ROOT_QUERY", {})
            .get("aemHeaderFooter", {})
            .get("header", {})
            .get("topnav", [])[0]
        )
        # print(nav)

        items = nav.get("items", [])

        for item in items:
            main_cate = item.get("title")
            if item.get("childrens"):
                for sub in (item.get("childrens") or []):
                    sub_cate = sub.get("title")
                    sub_children = sub.get("childrens")

                    if sub_children:
                        for sub_sub in (sub_children or []):
                            urls_data.append({
                                "main_category": main_cate,
                                "sub_category": sub_cate,
                                "sub_sub_category": sub_sub.get("title"),
                                "url": urljoin(base_url, sub_sub.get("url")),
                                "status": "pending"
                            })
                    else:
                        urls_data.append({
                            "main_category": main_cate,
                            "sub_category": sub_cate,
                            "sub_sub_category": "",
                            "url": urljoin(base_url, sub.get("url")),
                            "status": "pending"
                        })
            else:
                urls_data.append({
                    "main_category": main_cate,
                    "sub_category": "",
                    "sub_sub_category": "",
                    "url": urljoin(base_url, item.get("url")),
                    "status": "pending"
                })


    # with open ("merch2_json_data.json", "w" , encoding='utf-8') as f:
    #     # f.write(urls_data)
    #     json.dump(urls_data, f, indent=4)


    return urls_data


def next_page_url(parent_product_url):
    print("------next page ----")
    
    url_component = parent_product_url.replace(url, "").strip("/").split("/")

    next_url = parent_product_url + r"?country=AU&language=en"
    
    for element in url_component:
        next_url = next_url + r"&cmsRoute=" + element
    
    next_url = next_url + r"&page="
    return next_url




# ==============================
# RETRY FUNCTION
# ==============================
def fetch_with_retry(url, parent_product_id, start_page_num, retries=3 ):
    for attempt in range(retries):
        try:
            response = SESSION.get(url, timeout=180)

            if response.status_code == 200 and "__NEXT_DATA__" in response.text:
                return response

            print(f"[Retry] Error Bad response {response.status_code} | Attempt {attempt+1}", "url :", url,  "\nparent_product_id : ", parent_product_id , "start_page_num : ",start_page_num)

        except Exception as e:
            print(f"[Retry] Error Exception: {e} | Attempt {attempt+1}" , "url :", url,  "\nparent_product_id : ", parent_product_id , "start_page_num : ",start_page_num)

        # time.sleep(delay)

    return None



def single_category_url(product_url, start_page_num, next_url, items_data_list, parent_product_url, next_folder_path, parent_product_id, error_check=None):
    # print("------single_category_url ----", start_page_num)
    # print("second : ", product_url)
    
    if error_check:
        print("------true codition------", "parent_product_id : ", parent_product_id , "start_page_num : ",start_page_num)
        return error_check

    try:
        # response_data = SESSION.get(
        #         product_url,
        #         timeout=180
        #     )
        
        response_data = fetch_with_retry(product_url, parent_product_id, start_page_num)

        if not response_data:
            print(" errro Failed after retries", "parent_product_id : ", parent_product_id , "start_page_num : ",start_page_num)
            # update_merck_url_status(parent_product_id, "pending")
            # error_check = True
            return error_check
        
        pages_path = os.path.join(next_folder_path, str(start_page_num))
        # print("pages_path", pages_path)

        with open(f"{pages_path}.html.gz", "w", encoding='utf-8') as f:
            f.write(response_data.text)


        tree = html.fromstring(response_data.text)
        script_text = tree.xpath("//script[@id='__NEXT_DATA__']/text()")

        if not script_text:
            print(" error Script tag missing",  "parent_product_id : ", parent_product_id , "start_page_num : ",start_page_num)
            # update_merck_url_status(parent_product_id, "pending")
            return error_check
        
        raw_json = script_text[0].strip()


        # ==============================
        # VALIDATE JSON BEFORE LOAD
        # ==============================
        ### this is comment now........
        # if not raw_json.endswith("}"):
        #     print(" error Truncated JSON detected", "parent_product_id : ", parent_product_id , "start_page_num : ",start_page_num)

        #     with open(f"{pages_path}_broken.json", "w", encoding="utf-8") as f:
        #         f.write(raw_json)

        #     # update_merck_url_status(parent_product_id, "pending")
        #     return error_check
        
        #  Safe JSON load
        try:
            python_dict = json.loads(raw_json)
        except Exception as e:
            print("Error JSON load error:", e , "parent_product_id : ", parent_product_id , "start_page_num : ",start_page_num )
            error_check = True
            # update_merck_url_status(parent_product_id, "pending")

            return error_check
        
        root_query = python_dict.get("props", {}).get("apolloState", {}).get("ROOT_QUERY", {})

        data_block = None
        
        for key, value in root_query.items():
            if key.startswith("getProductSearchResults"):
                data_block = value
                break
        
        
        if data_block:
            numPages = data_block.get("metadata", {}).get("numPages")

            items = data_block.get("items", [])
            
            # print("Total items:", len(items))
            
            if items:
                for data in items:
                    try:
                        product_name = data.get("name")

                        product = data.get("__typename").lower()
                        brand_name = data.get("brand").get("key").lower()
                        productNumber = data.get("productNumber")

                        # print("Typename:", product, productNumber, brand_name)
                        product_url =  url + "/" + product + "/" + brand_name + "/" + productNumber

                        # print("table data :", product_name, brand_name, productNumber, product_url)

                        items_data_list.append({
                            "parent_id" : parent_product_id,
                            "sub_category_url" : parent_product_url,
                            "product_name" : product_name,
                            "brand_name" : brand_name,
                            "productNumber" : productNumber,
                            "product_url" : product_url
                        })
                        # print(items_data_list)

                        # break
                    except Exception as e:
                        print("Error Item parsing error:", e, "parent_product_id : ", parent_product_id , "start_page_num : ",start_page_num)
                        error_check = True
                        # update_merck_url_status(parent_product_id, "pending")
                        return error_check

                        # continue

            # INSERT CHUNK
            if len(items_data_list) >= 800:
                print("1 data insert into table : ", len(items_data_list))
                product_url_insert(items_data_list)
                items_data_list.clear()
                print("2 data insert into table : ", len(items_data_list))


            # if start_page_num == 2:
            #     return 
            start_page_num += 1
            # print("numPages : ", numPages)
            # print("start_page_num : ", start_page_num)

            if numPages and numPages >= start_page_num:
                product_url = next_url + str(start_page_num)
                error_check = single_category_url(product_url, start_page_num, next_url, items_data_list, parent_product_url, next_folder_path, parent_product_id, error_check)

        return error_check
    
    except Exception as e:
        print("Error Main function error:", e, "parent_product_id : ", parent_product_id , "start_page_num : ",start_page_num)
        error_check = True
        # update_merck_url_status(parent_product_id, "pending")

        return error_check



def child_product_url(list_data : list):
    print("-----child_product_url-----")
    folder_path = r"D:\vishal_kushvanshi\scrapy\merck_product_pages\sub_category_url_pages"
    os.makedirs(folder_path, exist_ok=True)

    count = 1
    for dict_data in list_data:
        items_data_list = []

        start_page_num = 1

        parent_product_id = dict_data.get("id")
        parent_product_url = dict_data.get("url")

        next_url = next_page_url(parent_product_url)

        next_folder_path = os.path.join(folder_path, str(parent_product_id))
        os.makedirs(next_folder_path, exist_ok=True)
        # print(next_folder_path)



        error_check = single_category_url(parent_product_url, start_page_num, next_url, items_data_list, parent_product_url, next_folder_path, parent_product_id)
        
        # print("items_data_list : ", items_data_list)
        print("items_data_list : ", len(items_data_list), ", parent id : ", parent_product_id, parent_product_url)
        # print(parent_product_url)


        product_url_insert(list_data=items_data_list)

        if not error_check:
            update_merck_url_status(parent_product_id, "success")



        # if count ==2:

        #     break
        # count += 1


from concurrent.futures import ThreadPoolExecutor, as_completed

def worker(list_data : list):
    # pass single dict as list
    # dynamic thread tuning generate.
    max_threads = min(8, len(list_data))   #  adjust (5–10 safe for scraping)

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(child_product_url, [data]) for data in list_data]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print("Thread error:", e)




    # child_product_url([dict_data])

