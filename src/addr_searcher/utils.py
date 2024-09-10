import csv

def load_data(source):
    if isinstance(source, str):
        if str.endswith(source, '.txt'):
            data = []
            with open(source, mode='r', encoding='utf-8') as file:
                data = [l for l in (line.strip() for line in file) if l]
    else:
        raise ValueError(f'Неподдерживаемый тип: {type(source)}')
    
    return data

def log_address_data(fname, log_data):
    with open(fname, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(log_data)