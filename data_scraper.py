import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import requests

def fetch_all_data(url, save_path):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)

    driver.get(url)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.js-navigation-container"))
    )

    # 定位到特定目录
    driver.find_element(By.XPATH, '//a[@href="/ruiduobao/shengshixian.com/tree/master/CTAmap%282013%E5%B9%B4-2023%E5%B9%B4%29%E8%A1%8C%E6%94%BF%E5%8C%BA%E5%88%92%E7%9F%A2%E9%87%8F"]').click()

    # 获取所有子目录和文件
    elements = driver.find_elements(By.CSS_SELECTOR, "div.js-navigation-container")
    for element in elements:
        items = element.find_elements(By.CSS_SELECTOR, "a.js-navigation-open")
        for item in items:
            file_url = item.get_attribute("href")
            file_name = file_url.split("/")[-1]

            if file_name == "README.md":
                continue

            download_url = f"{file_url}?raw=true"
            response = requests.get(download_url)
            print(download_url)
            with open(os.path.join(save_path, file_name), "wb") as f:
                f.write(response.content)

    driver.quit()

if __name__ == "__main__":
    url = "https://github.com/ruiduobao/shengshixian.com/tree/master/CTAmap%282013%E5%B9%B4-2023%E5%B9%B4%29%E8%A1%8C%E6%94%BF%E5%8C%BA%E5%88%92%E7%9F%A2%E9%87%8F"
    save_path = "./data"

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    fetch_all_data(url, save_path)
