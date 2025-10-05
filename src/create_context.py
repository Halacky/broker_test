import os
import shutil

def copy_py_files_to_text(directory_path, output_file, exclude_dirs=None):
    """
    Рекурсивно проходит по всем файлам в указанной директории,
    находит файлы с расширением .py и копирует их содержимое в текстовый файл.
    
    Args:
        directory_path (str): Путь к директории для поиска
        output_file (str): Путь к выходному текстовому файлу
        exclude_dirs (list): Список директорий для исключения
    """
    if exclude_dirs is None:
        exclude_dirs = []
    
    # Нормализуем имена исключаемых директорий для сравнения
    exclude_dirs = [os.path.normpath(dir_path) for dir_path in exclude_dirs]
    
    try:
        with open(output_file, 'w', encoding='utf-8') as out_file:
            # Рекурсивно проходим по всем файлам в директории
            for root, dirs, files in os.walk(directory_path):
                # Исключаем директории из списка exclude_dirs
                dirs[:] = [d for d in dirs if os.path.normpath(os.path.join(root, d)) not in exclude_dirs]
                
                for file in files:
                    if file.endswith('.py'):
                        file_path = os.path.join(root, file)
                        
                        # Проверяем, не находится ли файл в исключенной директории
                        if any(exclude_dir in os.path.normpath(file_path) for exclude_dir in exclude_dirs):
                            continue
                        
                        try:
                            # Читаем содержимое файла
                            with open(file_path, 'r', encoding='utf-8') as py_file:
                                content = py_file.read()
                            
                            # Записываем путь к файлу и его содержимое в выходной файл
                            out_file.write(f"file path: {file_path}\n")
                            out_file.write("```\n")
                            out_file.write(content)
                            out_file.write("\n```\n\n")
                            
                            print(f"Обработан файл: {file_path}")
                            
                        except Exception as e:
                            print(f"Ошибка при чтении файла {file_path}: {e}")
        
        print(f"\nВсе файлы .py успешно скопированы в {output_file}")
        
    except Exception as e:
        print(f"Ошибка при создании выходного файла: {e}")

def main():
    # Укажите путь к директории для поиска
    directory_to_search = input("Введите путь к директории для поиска: ").strip()
    
    # Укажите путь для выходного файла
    output_filename = input("Введите имя выходного файла (по умолчанию: python_files_output.txt): ").strip()
    if not output_filename:
        output_filename = "python_files_output.txt"
    
    # Запрос исключаемых директорий
    exclude_input = input("Введите директории для исключения (через запятую): ").strip()
    exclude_dirs = []
    if exclude_input:
        exclude_dirs = [dir_path.strip() for dir_path in exclude_input.split(',')]
    
    # Проверяем существование директории
    if not os.path.exists(directory_to_search):
        print(f"Ошибка: Директория '{directory_to_search}' не существует.")
        return
    
    # Запускаем процесс копирования
    copy_py_files_to_text(directory_to_search, output_filename, exclude_dirs)

# Версия с поддержкой шаблонов имен директорий
def copy_py_files_with_patterns(directory_path, output_file, exclude_patterns=None):
    """
    Версия с поддержкой шаблонов имен директорий (например, 'venv', '__pycache__')
    """
    if exclude_patterns is None:
        exclude_patterns = ['venv', '__pycache__', '.git', '.idea', 'node_modules']
    
    try:
        with open(output_file, 'w', encoding='utf-8') as out_file:
            for root, dirs, files in os.walk(directory_path):
                # Исключаем директории по шаблонам имен
                dirs[:] = [d for d in dirs if not any(pattern in d for pattern in exclude_patterns)]
                
                for file in files:
                    if file.endswith('.py'):
                        file_path = os.path.join(root, file)
                        
                        try:
                            with open(file_path, 'r', encoding='utf-8') as py_file:
                                content = py_file.read()
                            
                            out_file.write(f"file path: {file_path}\n")
                            out_file.write("```\n")
                            out_file.write(content)
                            out_file.write("\n```\n\n")
                            
                            print(f"Обработан файл: {file_path}")
                            
                        except Exception as e:
                            print(f"Ошибка при чтении файла {file_path}: {e}")
        
        print(f"\nВсе файлы .py успешно скопированы в {output_file}")
        
    except Exception as e:
        print(f"Ошибка при создании выходного файла: {e}")

# Компактная версия с исключениями
def collect_py_files(directory, output_file, exclude_dirs=None, exclude_patterns=None):
    """Компактная версия с поддержкой исключений"""
    if exclude_dirs is None:
        exclude_dirs = []
    if exclude_patterns is None:
        exclude_patterns = ['venv', '__pycache__', '.git']
    
    exclude_dirs = [os.path.normpath(dir_path) for dir_path in exclude_dirs]
    
    with open(output_file, 'w', encoding='utf-8') as out:
        for root, dirs, files in os.walk(directory):
            # Исключаем по полным путям
            dirs[:] = [d for d in dirs if os.path.normpath(os.path.join(root, d)) not in exclude_dirs]
            # Исключаем по шаблонам имен
            dirs[:] = [d for d in dirs if not any(pattern in d for pattern in exclude_patterns)]
            
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            out.write(f"file path: {file_path}\n```\n{f.read()}\n```\n\n")
                        print(f"✓ {file_path}")
                    except Exception as e:
                        print(f"✗ Ошибка в {file_path}: {e}")

# Примеры использования
if __name__ == "__main__":
    print("Выберите режим работы:")
    print("1 - Ручной ввод всех параметров")
    print("2 - Использование стандартных исключений (venv, __pycache__ и т.д.)")
    
    choice = input("Ваш выбор (1 или 2): ").strip()
    
    source_dir = input("Введите путь к директории: ").strip() or "."
    output_name = input("Введите имя выходного файла: ").strip() or "python_files.txt"
    
    if not os.path.exists(source_dir):
        print("Указанная директория не существует!")
        exit()
    
    if choice == "1":
        exclude_input = input("Введите директории для исключения (через запятую): ").strip()
        exclude_dirs = [dir_path.strip() for dir_path in exclude_input.split(',')] if exclude_input else []
        collect_py_files(source_dir, output_name, exclude_dirs=exclude_dirs)
    else:
        # Используем стандартные исключения для Python проектов
        exclude_patterns = ['venv', '__pycache__', '.git', '.idea', 'node_modules', 'build', 'dist']
        collect_py_files(source_dir, output_name, exclude_patterns=exclude_patterns)
    
    print(f"\nГотово! Результат в файле: {output_name}")