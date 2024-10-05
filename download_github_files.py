import requests
import os
import urllib.parse
import base64
token = os.environ.get('GITHUB_TOKEN')
def download_directory(owner, repo, path, local_dir, token=None):
    """
    递归下载GitHub仓库中的目录
    """
    # 构造API请求URL
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{urllib.parse.quote(path)}"
    headers = {}
    if token:
        headers['Authorization'] = f'token {token}'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        contents = response.json()
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        for item in contents:
            item_path = item['path']
            item_type = item['type']
            item_name = item['name']
            if item_type == 'dir':
                # 递归下载子目录
                download_directory(owner, repo, item_path, os.path.join(local_dir, item_name), token)
            elif item_type == 'file':
                # 下载文件
                download_file(item['download_url'], os.path.join(local_dir, item_name), token)
    else:
        print(f"无法访问 {url}, 状态码: {response.status_code}")

def download_file(url, local_path, token=None):
    """
    下载单个文件
    """
    headers = {}
    if token:
        headers['Authorization'] = f'token {token}'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(local_path, 'wb') as f:
            f.write(response.content)
        print(f"已下载文件: {local_path}")
    else:
        print(f"无法下载文件 {url}, 状态码: {response.status_code}")

if __name__ == "__main__":
    owner = "ruiduobao"
    repo = "shengshixian.com"
    path = "CTAmap(2013年-2023年)行政区划矢量"
    local_dir = "CTAmap(2013年-2023年)行政区划矢量"
    token = os.environ.get('GITHUB_TOKEN')  # 如果需要，可以在此处填写您的GitHub个人访问令牌
    download_directory(owner, repo, path, local_dir, token)