<<<<<<<<<<<<<<<<<<<<<<<< CodeGeeX Inline Diff>>>>>>>>>>>>>>>>>>>>>>>>
+import requests
+from bs4 import BeautifulSoup
+import os
+
+def download_dataset(url, save_path):
+    response = requests.get(url)
+    soup = BeautifulSoup(response.text, 'html.parser')
+    links = soup.find_all('a', href=True)
+
+    for link in links:
+        href = link['href']
+        if href.endswith('.zip'):
+            file_name = os.path.join(save_path, href.split('/')[-1])
+            with open(file_name, 'wb') as f:
+                f.write(requests.get(url + href).content)
+
+url = 'https://huggingface.co/datasets/'
+save_path = 'datasets'
+
+if not os.path.exists(save_path):
+    os.makedirs(save_path)
+
+download_dataset(url, save_path)

<<<<<<<<<<<<<<<<<<<<<<<< CodeGeeX Inline Diff>>>>>>>>>>>>>>>>>>>>>>>>