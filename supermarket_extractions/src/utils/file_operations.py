from pathlib import Path

def clean_folder(folder_path):
    if folder_path.exists():
        for file in folder_path.iterdir():
            file.unlink()

    folder_path.mkdir(parents=True, exist_ok=True)

def store_file(folder_path: Path, filename, input_data):
    
    folder_path.mkdir(parents=True, exist_ok=True)

    with open(folder_path / filename, 'w', encoding='utf-8') as f:
        f.write(input_data)