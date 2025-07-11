import csv
import os
from typing import List, Iterator, Any
import logging
import json
# from .utils import md5sum
import subprocess

class FileManager:
    def __init__(self, path):
        self.path = path
        self.tmp_path = path + '.tmp'

    def save_data(self, filename, new_data):
        """
        Atomically append data to a file with proper disk flushing.
        
        Args:
            filename: Target file to append to
            new_data: List of rows to append
        """
        ## checkpoints and 
        # Cambiar a modo append y revisar lineas con errores
        """ 
        TODO: Hacer append en vez de crear un nuevo archivo y remplazarlo. En caso de falla,
            chequear el ultimo batch a ver si es el mismo que tengo para ver si es duplicado.
            
        """
        with open(self.tmp_path, 'w+') as tmp_file:
            # Copy original file if it exists
            try:
                with open(filename, 'r') as original:
                    tmp_file.write(original.read())
            except FileNotFoundError:
                # File doesn't exist yet, that's fine for append
                pass
            
            # Append the new data
            writer = csv.writer(tmp_file, quoting=csv.QUOTE_MINIMAL)
            writer.writerows(new_data)
            
            # Ensure data is written to disk
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        
        # Atomic replacement
        os.rename(self.tmp_path, filename)
        return self.md5sum_system(filename)

    @classmethod
    def md5sum_system(cls, filename):
        result = subprocess.run(['md5sum', filename], capture_output=True, text=True)
        return result.stdout.split()[0]
    
    @classmethod
    def get_file_hash(self, filename):
        try:
            return self.md5sum_system(filename)
        except Exception as e:
            logging.error(f"Error getting file hash for {filename}: {str(e)}")
            return None

    def read(self) -> Iterator[List[Any]]:
        """
        Read the file and yield each row as a list.
        
        Returns:
            An iterator that yields rows from the file.
        """
        try:
            with open(self.path, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    yield row
        except (FileNotFoundError, IOError):
            return iter([])  # Return an empty iterator if the file does not exist or there is an I/O error
        
    @classmethod
    def clean_temp_files(self, files_to_remove):
        """Elimina los archivos temporales creados para un cliente"""
        for file in files_to_remove:
            try:
                if os.path.exists(file):
                    os.remove(file)
                    logging.info(f"action: clean_temp_files | file: {file} | result: removed")
            except Exception as e:
                logging.error(f"action: clean_temp_files | file: {file} | error: {str(e)}")

    @classmethod
    def load_state(self, filename):
        """Cargar el estado de un archivo JSON"""
        try:
            with open(filename, 'r+') as f:
                state = json.load(f)
                return {int(k): v for k, v in state.items()}
        except (FileNotFoundError, json.JSONDecodeError) as e:
            return {}
        
    def save_state(self, data):
        with open(self.tmp_path, 'w+') as f:
            f.write(data)
            f.flush()
        os.rename(self.tmp_path, self.path)
