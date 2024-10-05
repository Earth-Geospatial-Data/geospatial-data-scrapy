import requests
from bs4 import BeautifulSoup
import os
import sys

def download_file(url, local_filename):
    """
    下载文件并显示下载进度
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_length = r.headers.get('content-length')

        with open(local_filename, 'wb') as f:
            if total_length is None:
                f.write(r.content)
            else:
                dl = 0
                total_length = int(total_length)
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        dl += len(chunk)
                        done = int(50 * dl / total_length)
                        sys.stdout.write(f"\r[{('=' * done):50s}] {dl/total_length:.2%}")
                        sys.stdout.flush()
    print(f"\n已下载文件：{local_filename}")

def scrape_and_download(base_url, download_dir, file_types=None):
    """
    爬取下载链接并下载文件
    """
    response = requests.get(base_url)
    if response.status_code != 200:
        print(f"无法访问 {base_url}, 状态码: {response.status_code}")
        return

    soup = BeautifulSoup(response.content, 'lxml')

    # 创建下载目录
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    
    # 查找所有包含下载链接的 <a> 标签
    download_links = []
    for td in soup.find_all('td', class_='subregion'):
        # for ah in td.find_all('a'):
        link = td.find('a')
        print(f"下载链接:{link}")
        if link and 'href' in link.attrs:
            href = link['href']
            if href.endswith(tuple(file_types)):
                full_url = requests.compat.urljoin(base_url, href)
                download_links.append((full_url, href))    

    # 如果未找到任何下载链接
    if not download_links:
        print("未找到任何下载链接。")
        return

    # 下载所有链接的文件
    for full_url, href in download_links:
        filename = os.path.basename(href)
        local_path = os.path.join(download_dir, filename)
        print(f"正在下载 {filename} ...")
        download_file(full_url, local_path)

    print("所有文件已下载完成。")

if __name__ == "__main__":
    base_url = "https://download.geofabrik.de/asia/china.html"
    download_dir = "geofabrik_china_osm_data"
    # 指定要下载的文件类型
    file_types = ['.osm.pbf', '.shp.zip', '.osm.bz2']
    scrape_and_download(base_url, download_dir, file_types)