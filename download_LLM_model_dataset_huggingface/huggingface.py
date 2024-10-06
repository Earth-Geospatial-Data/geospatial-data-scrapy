from datasets import load_dataset
from huggingface_hub import snapshot_download
import json
import asyncio
def get_model(target_repo_id):
    print(f'-----------------------begin get_model :{target_repo_id} --------------------')
    m_name = target_repo_id.replace("/", "-")
    snapshot_download(repo_id=target_repo_id, local_dir=f'./GPT/models/{m_name}')
def get_dataset(t_dataset=""):
    target_dataset = t_dataset
   
    #data_files = {"train": "train.csv", "test": "test.csv", "validation": "validation.csv"}
    d_name = target_dataset.replace("/", "-") 
 
    try:
      print(f'-----------------------begin get_dataset: {target_dataset} --------------------')
      dataset = load_dataset(target_dataset,cache_dir=f'./datasets/{d_name}')
    except ZeroDivisionError as e:
      print(e)
      
    finally:  
      print('The try except is finished')   
async def get_ds(repo_path):
    repo_name = repo_path.replace("/", "-")
    try:
      print(f'-----------------------begin get_dataset: {repo_path} --------------------')
      dataset = load_dataset(repo_path,cache_dir=f'./cache_datasets/{repo_name}')
    except ZeroDivisionError as e:
      print(e)
    finally:  
      print(f'download {repo_path}The try except is finished')   
async def get_models(repo_path):
    repo_name = repo_path.replace("/", "-")
    try:
      print(f'-----------------------begin download model: {repo_path} --------------------')
      snapshot_download(repo_path,cache_dir=f'./models/{repo_name}')
    except ZeroDivisionError as e:
      print(e)
    finally:  
      print(f'download {repo_path}The try except is finished')         
if __name__ == "__main__":
    datasets = []
    models = []
    is_model = True
    is_ds = False
    ds_path = "./hf_ds.json"
    ms_path = "./hf_model.json"
    loop = asyncio.get_event_loop()
    get_ds_task = None
    get_model_task = None
    with open(ds_path, 'r') as f:
        datasets = json.load(f)
        print(datasets)
        f.close()
    
        
    with open(ms_path, 'r') as f:
        models = json.load(f)
        print(models)
        f.close()

    if is_ds is True:
      get_ds_task = [get_ds(repo["ds_name"]) for repo in datasets]
      loop.run_until_complete(asyncio.gather(*get_ds_task))

    if is_model is True:
      get_model_task = [get_models(repo["model_name"]) for repo in models]
      loop.run_until_complete(asyncio.gather(*get_model_task))
