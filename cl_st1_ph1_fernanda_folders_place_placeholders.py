import os

def place_placeholders(root_dir):
    for current_path, dirs, files in os.walk(root_dir):
        # If there are no subdirectories, it's a leaf
        if not dirs:
            placeholder_path = os.path.join(current_path, 'placeholder.txt')
            try:
                with open(placeholder_path, 'w', encoding='utf-8') as f:
                    f.write('placeholder')
                print(f'Created: {placeholder_path}')
            except OSError as e:
                print(f'Failed to create file at {placeholder_path}: {e}')

# Example usage
if __name__ == '__main__':
    # Replace with the path you want to scan
    root_directory = 'C:\\Users\\eyamr\\Downloads\\work\\cl_st1_fernanda\\cl_st1_ph1_fernanda_folders'
    place_placeholders(root_directory)