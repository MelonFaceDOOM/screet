import re
import os
import csv
import io 


def get_file_paths_from_folder(folder_path):
    file_paths = []
    for subdir, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(folder_path, file)
            file_paths.append(file_path)
    return file_paths


def get_file_names_and_paths_from_folder(folder_path):
    # also looks in subfolders
    file_names_and_paths = []
    for subdir, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(subdir, file)
            file_names_and_paths.append([file, file_path])
    return file_names_and_paths


def get_only_file_paths_from_folder(folder_path):
    # ignores subfolders
    list_files = []
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    for filename in files:
        joined = os.path.join(folder_path, filename)
        list_files.append(joined)
    return list_files


def flatten_obj_list(obj_list, headers=True):
    rows = []
    cols = obj_list[0].attrs()
    if headers:
        rows.append(cols)
    for obj in obj_list:
        obj_data = [getattr(obj, col)for col in cols]
        rows.append(obj_data)
    return rows
    
    
def clean_csv_value(value):
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')
    

def csv_object_from_obj_list(objs):
    cols = objs[0].attrs()
    csv_object = io.StringIO()
    writer = csv.writer(csv_object)
    for obj in objs:
        data = list(map(clean_csv_value, [getattr(obj, col) for col in cols]))
        writer.writerow(data)
    csv_object.seek(0)
    return csv_object
    
    
def objs_to_csv(obj_list, output_file_name):
    flat_objs = flatten_obj_list(obj_list)
    with open(output_file_name, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(flat_objs)


def minimal_url(url):
    """removes http://, https://, and www."""
    pattern = '(?:https?:\/\/)?(?:www\.)?((?:[\w-]+\.)+(?:\w+)+.*)'
    match = re.match(pattern, url)
    if not match:
        return None
    return match.group(1)


def extract_domain(url):
    pattern = '(?:https?:\/\/)?(?:www\.)?((?:[\w-]+\.)+(?:\w+)+).*'
    match = re.match(pattern, url)
    if not match:
        return None
    return match.group(1)
    
    
def chunk_deque(dq, n_chunks):
    """Yield successive n-sized chunks from lst."""
    chunks = []
    chunk_size = int(len(dq)/n_chunks) + 1
    for n in range(n_chunks):
        chunk = deque()
        for i in range(chunk_size):
            try:
                chunk.append(dq.popleft())
            except IndexError:
                break
        chunks.append(chunk)
    return chunks
