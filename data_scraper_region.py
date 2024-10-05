from multiprocessing import Pool
import requests
import base64
import os

# Create a session object to reuse TCP connections
session = requests.Session()

def fetch_file_content(repo_owner, repo_name, file_path, local_file_path,  access_token=None):
    """
    Fetches the content of a file from a GitHub repository.

    Parameters:
    - repo_owner: The owner of the repository.
    - repo_name: The name of the repository.
    - file_path: The path to the file in the repository.
    - access_token: Personal access token for GitHub API (optional).

    Returns:
    The decoded content of the file.
    """
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{file_path}"
    headers = {}

    if access_token:
        headers['Authorization'] = f'token {access_token}'

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        with open(local_file_path, 'wb') as file:
            file.write(response.content)
        print(f"File {file_path} downloaded successfully.")
    else:
        print(f"Failed to download file {file_path}. Status code: {response.status_code}")
def get_github_directory_contents(repo_owner, repo_name, directory_path, year,access_token=None):
    """
    Fetches the contents of a directory from a GitHub repository.

    Parameters:
    - repo_owner: The owner of the repository.
    - repo_name: The name of the repository.
    - directory_path: The path to the directory in the repository.
    - access_token: Personal access token for GitHub API (optional).

    Returns:
    A list of files and directories in the specified directory.
    """
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{directory_path}"
    headers = {}

    if access_token:
        headers['Authorization'] = f'token {access_token}'

    response = session.get(url, headers=headers)

    if response.status_code == 200:
        contents = response.json()
        local_dir = os.path.join("./data", directory_path)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        if isinstance(contents, list):
            for item in contents:
                item_path = item['path']
                item_type = item['type']
                item_name = item['name']
                if item_type == "file":
                    fetch_file_content(
                        repo_owner, repo_name, item_path, os.path.join(local_dir, item_name), access_token
                    )
                    # if file_content is not None:
                    #     local_file_path = f"./data/{year}/{item['name']}"
                    #     # save_file_content(local_file_path, file_content)
                    #     print(f"Saved {item['name']} to {local_file_path}")
                if item_type == "dir":
                    get_github_directory_contents(
                        repo_owner, repo_name, os.path.join(directory_path, item_name), year, access_token
                    )

        else:
            print(f"Error fetching directory contents: {contents}")
    else:
        print(f"无法访问 {url}, 状态码: {response.status_code}")

def save_file_content(local_path, content):
    """
    Saves content to a file in the specified local path.

    Parameters:
    - local_path: The local file path where the content will be saved.
    - content: The content to save.
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'w', encoding='utf-8') as file:
        file.write(content)

def fetch_yearly_data(repo_owner, repo_name, start_year, end_year, base_directory_path, access_token=None):
    """
    Fetches and saves files from yearly directories within a specified range.
    """
    # years = range(start_year, end_year + 1)
    # directory_path = f"{base_directory_path}/{year}年"
    # with Pool(processes=len(years)) as pool:
    #     pool.map(lambda year: get_github_directory_contents(repo_owner, repo_name, f"{base_directory_path}/{year}年", year, access_token), years)
    for year in range(start_year, end_year + 1):
        directory_path = f"{base_directory_path}/{year}年"
        get_github_directory_contents(repo_owner, repo_name, directory_path, year, access_token)
        print(f"get contents of year {year} of {directory_path}")


# Example usage
if __name__ == "__main__":
    repo_owner = "ruiduobao"
    repo_name = "shengshixian.com"
    base_directory_path = "CTAmap(2013年-2023年)行政区划矢量"
    start_year = 2013
    end_year = 2023
    # Include your GitHub token here if needed
    access_token = os.environ.get('GITHUB_TOKEN')

    fetch_yearly_data(repo_owner, repo_name, start_year, end_year, base_directory_path, access_token)