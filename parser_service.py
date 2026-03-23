import tempfile
from bs4 import BeautifulSoup
import os
import glob
import sqlite3
import json
from tqdm import tqdm
import rarfile
import shutil
import re
from concurrent.futures import ThreadPoolExecutor, as_completed


class MessageParser:
    def __init__(self):
        self.date_format = "%d.%m.%Y %H:%M"

    def parse_date_time(self, date_time_str):
        try:
            date_time_obj = datetime.strptime(date_time_str, self.date_format)
            date_only = date_time_obj.date()
            time_only = date_time_obj.time()
            return {"date": date_only, "time": time_only}
        except ValueError as e:
            return {"error": str(e)}
        
    def extract_number(self,file_path):
        try:
            return int(''.join(filter(str.isdigit, os.path.basename(file_path))))
        except ValueError:
            return float('inf')

class MessageExtractor(MessageParser):
    def __init__(self):
        super().__init__()

    def get_author_blocks(self, soup):
        try:
            return soup.find('div', class_='wrapped')
        except Exception as e:
            print(f'Ошибка при получении блоков: {e}')
            return None

    def get_im_in_blocks(self, soup):
        try:
            return soup.find_all('div', class_='im_in')
        except Exception as e:
            print(f'Ошибка при получении блоков: {e}')
            return []

    def get_name(self, block):
        try:
            return block.find('a', class_='mem_link').text.strip()
        except Exception as e:
            print(f'Ошибка при получении имени: {e}')
            return None

    def get_user_id(self, href_value):
        try:
            return href_value.replace('https://vk.com/id', '')
        except Exception as e:
            print(f'Ошибка при извлечении user_id: {e}')
            return None

    def get_message_date(self, block):
        try:
            date_element = block.find('div', class_='im_log_date').find('a', class_='im_date_link')
            message_date = date_element.text.strip() if date_element else None
            return message_date
        except Exception as e:
            print(f'Ошибка при извлечении message_date: {e}')
            return None

    def get_message(self, block):
        try:
            message_text = ''.join([str(item) for item in block.contents if isinstance(item, str)])
            message_text = message_text.strip()
            return message_text
        except Exception as e:
            print(f'Ошибка при получении сообщения: {e}')
            return None

    def get_attachment_links(self, block): 
        try: 
            gallery_attachments = block.find_all('div', class_='gallery attachment')
            attachment_links = [a['href'] for gallery_attachment in gallery_attachments for a in gallery_attachment.find_all('a', class_='download_photo_type')]
            return attachment_links
        except Exception as e:
            print(f'Ошибка при получении сообщения: {e}')
            return []

class DetailsChat(): 
    def __init__(self):
        super().__init__()

    def details(self, file_path):
        return {
            "chat_id": self._get_last_folder(file_path),  
            "file_chat": str(file_path)
        }
    
    @staticmethod
    def _get_last_folder(path):
        folder_name = os.path.basename(os.path.dirname(path))
        match = re.search(r'\(id(\d+)\)', folder_name)
        if match:
            return match.group(1)
        else:
            return None


class MessageProcessor(MessageExtractor):
    def __init__(self):
        super().__init__()

    def process_block(self, block):
        try:
            author_block = self.get_author_blocks(block)
            author_name = self.get_name(author_block)
            href_value = author_block.find('a', class_='mem_link')['href']
            author_link = self.get_user_id(href_value)
            message_text = self.get_message(author_block)
            message_date = self.get_message_date(block)
            attachment_links = self.get_attachment_links(block)
            # print(f'Имя: {author_name}\nСсылка: {author_link}\nСообщение: {message_text}\n\Дата: {message_date}\n{attachment_links}\n')
            return {
                "author_name": author_name,
                "author_link": author_link,
                "message_text": message_text,
                "message_date": message_date,
                "attachment_links": attachment_links
            }
        except Exception as e:
            print(f'Ошибка при обработке блока: {e}')

morph = None
def get_morph():
    global morph
    if morph is None:
        import pymorphy3
        morph = pymorphy3.MorphAnalyzer()
    return morph

def lemmatize_text(text):
    if not text:
        return ""
    m = get_morph()
    words = re.findall(r'[а-яА-ЯёЁa-zA-Z0-9]+', str(text).lower())
    return " ".join([m.parse(w)[0].normal_form for w in words])

def lemmatize_chunk(chunk):
    """Оптимизированная пакетная лемматизация для многопоточности"""
    m = get_morph()
    res = []
    for r_id, text in chunk:
        if not text:
            res.append((r_id, "", ""))
            continue
        words = re.findall(r'[а-яА-ЯёЁa-zA-Z0-9]+', str(text).lower())
        res.append((r_id, str(text), " ".join([m.parse(w)[0].normal_form for w in words])))
    return res

def process_single_file(file_path):
    try:
        from bs4 import BeautifulSoup
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()

        details_chat = DetailsChat()
        file_details = details_chat.details(file_path)

        # Using lxml for maximum parsing speed (about 5-10x faster than html.parser)
        soup = BeautifulSoup(content, 'lxml')
        processor = MessageProcessor()
        im_in_blocks = processor.get_im_in_blocks(soup)
        
        results = []
        for block in im_in_blocks:
            details_copy = file_details.copy()
            processed_block = processor.process_block(block)
            if processed_block:
                processed_block['message_lemmatized'] = lemmatize_text(processed_block.get('message_text', ''))
                details_copy.update(processed_block)
            results.append(details_copy)
        return results
    except Exception as e:
        print(f'Ошибка при обработке файла {file_path}: {e}')
        return []

class MessageFileProcessor(MessageProcessor):
    def __init__(self):
        super().__init__()
        self.details = {}  
        self.for_append = []
        self.id_msg = 1

    def process_archive_file(self, archive_file, output_dir):
        try:
            if archive_file.lower().endswith('.zip'):
                import zipfile
                with zipfile.ZipFile(archive_file, 'r') as zf:
                    zf.extractall(output_dir)
            else:
                rf = rarfile.RarFile(archive_file)
                rf.extractall(output_dir)
        except Exception as e:
            error_msg = str(e)
            if "Cannot find working tool" in error_msg:
                raise Exception("Утилита UnRAR не найдена в системе. Пожалуйста, используйте формат архива .ZIP при запросе архива у ВКонтакте, либо установите системную утилиту UnRAR.")
            raise Exception(f'Ошибка при разархивации файла {archive_file}: {e}')

    def process_all_html_files(self, archive_file, output_dir):
        import multiprocessing
        from concurrent.futures import ProcessPoolExecutor, as_completed

        # Создаем временную папку для извлечения содержимого архива
        temp_dir = tempfile.mkdtemp()

        # Распаковываем архив
        try:
            self.process_archive_file(archive_file, temp_dir)
        except Exception as e:
            shutil.rmtree(temp_dir)
            raise e

        # Получаем список всех файлов с расширением 'htm' во всех подкаталогах временной папки
        files = glob.glob(os.path.join(temp_dir, '**', '*.htm*'), recursive=True)

        util = MessageParser()
        sorted_files = sorted(files, key=lambda x: (os.path.dirname(x), util.extract_number(x)))

        # Используем ProcessPoolExecutor для обхода GIL и максимальной скорости
        max_workers = max(1, multiprocessing.cpu_count() - 1)
        all_results = []
        
        if sorted_files:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_single_file, file_path) for file_path in sorted_files]

                for future in tqdm(as_completed(futures), total=len(sorted_files), desc="Processing files", unit="file"):
                    try:
                        res = future.result()
                        if res:
                            all_results.extend(res)
                    except Exception as e:
                        print(f'Ошибка при обработке: {e}')

        # Удаляем временную папку после использования
        shutil.rmtree(temp_dir)

        # Назначаем ID последовательно
        for idx, item in enumerate(all_results, start=1):
            item['id'] = idx

        self.for_append = all_results
        return self.for_append

class MessageDatabase:
    def __init__(self, db_name, table_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.table_name = table_name
        self.create_table()

    def create_table(self):
        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER NOT NULL,  
                chat_id TEXT,
                file_chat TEXT,
                author_name TEXT,
                author_link TEXT,
                message_text TEXT,
                message_date TEXT,
                attachment_links TEXT
            )
        ''')

    def insert_data(self, json_data):
        self.cursor.executemany(f'''
            INSERT INTO {self.table_name} (
                id, chat_id, file_chat, author_name, author_link, message_text, message_date, attachment_links
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            (
                item['id'],
                item['chat_id'],
                item['file_chat'],
                item['author_name'],
                item['author_link'],
                item['message_text'],
                item['message_date'],
                json.dumps(item.get('attachment_links', []))
            ) for item in json_data
        ])

    def commit_and_close(self):
        self.conn.commit()
        self.conn.close()

class JsonArchiveProcessor(MessageFileProcessor):
    def __init__(self):
        super().__init__()

    def process_single_json_file(self, file_path):
        import json
        from datetime import datetime
        try:
            filename = os.path.basename(file_path)
            chat_id = filename.replace('.json', '')
            
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_text = f.read().strip()
                
            # Очищаем от JavaScript префиксов, если они есть (например 'let dialogjson = { ... }' или просто 'var ...')
            start_idx = raw_text.find('{')
            end_idx = raw_text.rfind('}')
            
            if start_idx != -1 and end_idx != -1:
                clean_json_str = raw_text[start_idx:end_idx+1]
                data = json.loads(clean_json_str)
            else:
                return []
                
            users = {}
            if 'profiles' in data and isinstance(data['profiles'], list):
                for profile in data['profiles']:
                    if not isinstance(profile, dict): continue
                    user_id = profile.get('id')
                    if not user_id: continue
                    users[str(user_id)] = f"{profile.get('firstName', '')} {profile.get('lastName', '')}".strip() or f"User_{user_id}"
                    
            results = []
            if 'messages' in data and isinstance(data['messages'], list):
                messages = sorted(data['messages'], key=lambda x: x.get('time', 0))
                
                for msg in messages:
                    if not isinstance(msg, dict): continue
                    
                    user_id = str(msg.get('from', ''))
                    if not user_id: continue
                    
                    author_name = users.get(user_id, f"User_{user_id}")
                    message_text = str(msg.get('text', '')).strip()
                    
                    # Convert UNIX time
                    unix_time = msg.get('time', 0)
                    try:
                        message_date = datetime.fromtimestamp(unix_time).strftime('%d.%m.%Y %H:%M')
                    except Exception:
                        message_date = "01.01.1970 00:00"
                        
                    # Parse photos
                    attachment_links = []
                    if 'attachments' in msg and isinstance(msg['attachments'], list):
                        for attach_data in msg['attachments']:
                            if isinstance(attach_data, dict) and attach_data.get('type') == 'photo':
                                photo_data = attach_data.get('photo', attach_data)
                                if isinstance(photo_data, dict):
                                    for field in ['hd', 'url', 'src', 'photo_1280', 'photo_807', 'photo_604']:
                                        if field in photo_data and photo_data[field]:
                                            attachment_links.append(photo_data[field])
                                            break
                            
                    results.append({
                        "chat_id": chat_id,
                        "file_chat": str(file_path),
                        "author_name": author_name,
                        "author_link": user_id,
                        "message_text": message_text,
                        "message_date": message_date,
                        "attachment_links": attachment_links
                    })
            return results
        except Exception as e:
            print(f"Ошибка при обработке JSON файла {file_path}: {e}")
            return []

    def process_all_json_files(self, archive_file, output_dir):
        import shutil
        import tempfile
        from tqdm import tqdm
        
        temp_dir = tempfile.mkdtemp()
        try:
            self.process_archive_file(archive_file, temp_dir)
        except Exception as e:
            shutil.rmtree(temp_dir)
            raise e

        files = glob.glob(os.path.join(temp_dir, '**', '*.json'), recursive=True)
        
        all_results = []
        if files:
            # We skip ProcessPoolExecutor here because JSON parsing is wildly CPU-efficient natively
            # Sequential/Threaded parsing avoids process spawning overheads for small JSONs.
            for file_path in tqdm(files, desc="Processing JSON files", unit="file"):
                res = self.process_single_json_file(file_path)
                if res:
                    all_results.extend(res)
                    
        shutil.rmtree(temp_dir)
        
        import multiprocessing
        from concurrent.futures import ProcessPoolExecutor, as_completed
        
        # Лемматизация требует ProcessPool (очень тяжелая)
        if all_results:
            chunk_size = max(1, len(all_results) // multiprocessing.cpu_count())
            chunks = []
            current_chunk = []
            
            # Назначаем ID
            for idx, item in enumerate(all_results, start=1):
                item['id'] = idx
                current_chunk.append((idx, item['message_text']))
                if len(current_chunk) >= chunk_size:
                    chunks.append(current_chunk)
                    current_chunk = []
            if current_chunk:
                chunks.append(current_chunk)
                
            lemmatized_map = {}
            with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
                futures = {executor.submit(lemmatize_chunk, chunk): chunk for chunk in chunks}
                for future in tqdm(as_completed(futures), total=len(chunks), desc="Lemmatizing text"):
                    for r_id, orig, lemmatized in future.result():
                        lemmatized_map[r_id] = lemmatized
                        
            for item in all_results:
                item['message_lemmatized'] = lemmatized_map.get(item['id'], '')

        return all_results
