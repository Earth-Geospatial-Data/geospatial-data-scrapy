# from datasets import load_dataset
from huggingface_hub import snapshot_download
import json
def get_model(target_repo_id):
    print(f'-----------------------begin get_model :{target_repo_id} --------------------')
    m_name = target_repo_id.split("/")
    snapshot_download(repo_id=target_repo_id, local_dir=f'./models/{m_name[0]}/{m_name[1]}')
def get_dataset(t_dataset=""):
    target_dataset = t_dataset
   
    #data_files = {"train": "train.csv", "test": "test.csv", "validation": "validation.csv"}
    d_name = target_dataset.replace("/", "-") 
 
    try:
      print(f'-----------------------begin get_dataset: {target_dataset} --------------------')
      dataset = load_dataset(target_dataset,cache_dir=f'./cache_datasets/{d_name}')
    except ZeroDivisionError as e:
      print(e)
      
    finally:  
      print('The try except is finished')   
       
if __name__ == "__main__":
    datasets = []
    models = []
    is_model = True
    is_ds = False
    with open('./dataset.json', 'r') as f:
        datasets = json.load(f)
        print(datasets)
        f.close()
    
        
    with open('./models.json', 'r') as f:
        models = json.load(f)
        print(models)
        f.close()
    if is_model is True: 
        for ml in models:
          print(ml['model_name'])
          get_model(ml['model_name'])
    if is_ds is True:
        for ml in datasets:
            print(ml['ds_name'])
            get_dataset(ml['ds_name']) 
