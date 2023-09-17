monitor_data = {
    'Date': None,
    'yad2': {
        'Total_size':None,
        'New_data':None,
        'status':None,
    },
    'nadlan': {
        'Total_size':None,
        'New_data':None,
        'status':None,
    },
    'madlan': {
        'Total_size':None,
        'New_data':None,
        'status':None,
        'error':None,
    },
    'Clean': {
        'nadlan': {
            'Total_size': None,
            'status': None,
            'error': None,
        },
        'madlan':{
            'Total_size': None,
            'status': None,
            'error': None,
        },
        'yad2':{
            'Total_size': None,
            'status': None,
            'error': None,
        },
    },
    'algo': {
        'nadlan': {
            'r2': None,
            'mae': None,
            'status': None,
            'error': None,
        },
        'madlan':{
            'r2': None,
            'mae': None,
            'status': None,
            'error': None,
        },
        'yad2':{
            'r2': None,
            'mae': None,
            'status': None,
            'error': None,
        },
    },

}
def find_errors(data, errors, path=""):
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = f"{path} -> {key}" if path else key
            if key == "error" and value is not None:
                errors[new_path] = value
                print(f"{new_path} : {value}")
            find_errors(value, errors, new_path)


