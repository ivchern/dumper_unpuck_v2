from flask import Flask, render_template, request, jsonify
import sqlite3
import os
from werkzeug.utils import secure_filename
from parser_service import MessageFileProcessor
import re
import json

app = Flask(__name__)
DATABASE = 'messages.db'
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'rar', 'zip'}


class MessageDatabase:
    def __init__(self, db_name):
        self.db_name = db_name

    def create_table(self, table_name):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
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
            # FTS5 Indexing corresponding to the table (Contentless standalone table for custom lemmatization inserts)
            cursor.execute(f'''
                CREATE VIRTUAL TABLE IF NOT EXISTS {table_name}_fts USING fts5(
                    message_text_raw,
                    message_text_lemma
                )
            ''')

    def insert_data(self, json_data, table_name):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.executemany(f'''
                INSERT INTO {table_name} (
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
            cursor.executemany(f'''
                INSERT OR REPLACE INTO {table_name}_fts (rowid, message_text_raw, message_text_lemma)
                VALUES (?, ?, ?)
            ''', [
                (
                    item['id'],
                    item.get('message_text', ''),
                    item.get('message_lemmatized', '')
                ) for item in json_data
            ])

    def commit_and_close(self):
        pass  # You can keep this method empty since closing the connection after each operation

    def execute_query(self, query):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
        return results

db = MessageDatabase(DATABASE)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def normalize_database_name(name):
    translit_dict = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
    }
    
    # Транслитерируем кириллицу в латиницу (заодно переведя в нижний регистр)
    name = name.lower()
    transliterated = ''.join([translit_dict.get(c, c) for c in name])
    
    # Заменяем все недопустимые символы на '_'
    normalized = re.sub(r'[^0-9a-z$_]', '_', transliterated)
    
    # Убираем лишние подряд идущие подчеркивания и по краям
    normalized = re.sub(r'_+', '_', normalized).strip('_')
    
    if not normalized:
        normalized = 'default_table'
        
    # Имя таблицы не может начинаться с цифры в SQLite
    if normalized[0].isdigit():
        normalized = 'table_' + normalized
        
    return normalized


@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'})

    name_table = os.path.splitext(file.filename)[0].lower()
    name_table = normalize_database_name(name_table)

    if file and allowed_file(file.filename):
        ext = file.filename.rsplit('.', 1)[1].lower()
        filename = f"{name_table}.{ext}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)

        processor = MessageFileProcessor()
        try:
            json_data = processor.process_all_html_files(file_path, app.config['UPLOAD_FOLDER'])
        except Exception as e:
            if os.path.exists(file_path):
                os.remove(file_path)
            return jsonify({'error': str(e)})

        db.create_table(name_table)
        db.insert_data(json_data, name_table)
        setup_fts_for_existing_tables()

        return jsonify({'success': 'File uploaded and processed successfully'})

    return jsonify({'error': 'Invalid file extension'})


@app.route('/')
def index():
    table_list = get_table_list()

    if request.method == 'POST':
        if 'file' not in request.files:
            return render_template('index.html', table_list=table_list, upload_status='No file part')

        file = request.files['file']

        if file.filename == '':
            return render_template('index.html', table_list=table_list, upload_status='No selected file')

        if file and allowed_file(file.filename):
            ext = file.filename.rsplit('.', 1)[1].lower()
            table_name = normalize_database_name(os.path.splitext(file.filename)[0].lower())
            filename = f"{table_name}.{ext}"
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            processor = MessageFileProcessor()
            try:
                json_data = processor.process_all_html_files(file_path, 'uploads')
            except Exception as e:
                if os.path.exists(file_path):
                    os.remove(file_path)
                return render_template('index.html', table_list=table_list, upload_status=f'Ошибка: {e}')

            db.create_table(table_name)
            db.insert_data(json_data, table_name)
            db.commit_and_close()
            setup_fts_for_existing_tables()

            upload_status = 'File uploaded and processed successfully'
        else:
            upload_status = 'Invalid file extension'

        return render_template('index.html', table_list=table_list, upload_status=upload_status)

    return render_template('index.html', table_list=table_list, upload_status=None)

@app.route('/upload_json', methods=['POST'])
def upload_json():
    table_list = get_table_list()
    if 'file' not in request.files:
        return render_template('index.html', table_list=table_list, upload_status='No file part')

    file = request.files['file']
    if file.filename == '':
        return render_template('index.html', table_list=table_list, upload_status='No selected file')

    if file and allowed_file(file.filename):
        ext = file.filename.rsplit('.', 1)[1].lower()
        table_name = normalize_database_name(os.path.splitext(file.filename)[0].lower() + "_json")
        filename = f"{table_name}.{ext}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)

        from parser_service import JsonArchiveProcessor
        processor = JsonArchiveProcessor()
        try:
            json_data = processor.process_all_json_files(file_path, app.config['UPLOAD_FOLDER'])
        except Exception as e:
            if os.path.exists(file_path):
                os.remove(file_path)
            return render_template('index.html', table_list=table_list, upload_status=f'Ошибка загрузки JSON: {e}')

        db.create_table(table_name)
        db.insert_data(json_data, table_name)
        setup_fts_for_existing_tables()

        return render_template('index.html', table_list=get_table_list(), upload_status='JSON Archive unpacked and Search FTS Index mapped successfully!')

    return render_template('index.html', table_list=table_list, upload_status='Invalid file extension')


@app.route('/query', methods=['POST'])
def query():
    user_query = request.form['query']
    results = db.execute_query(user_query)
    table_name = extract_table_name(user_query)
    return render_template('result.html', results=results, my_table=table_name)


@app.route('/manual_index', methods=['POST'])
def manual_index():
    try:
        setup_fts_for_existing_tables()
        return jsonify({'success': 'Ручная индексация успешно завершена! Все таблицы добавлены в умный поиск.'})
    except Exception as e:
        return jsonify({'error': f'Ошибка индексации: {e}'})


@app.route('/deduplicate_tables', methods=['POST'])
def deduplicate_tables():
    """
    Умное удаление дубликатов таблиц (если один и тот же архив загрузили дважды).
    Определяет дубликаты по точному совпадению количества строк, диапазону ID и chat_id.
    """
    try:
        with sqlite3.connect(DATABASE, timeout=60.0) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '%_fts%' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_fts_%'")
            tables = [row[0] for row in cursor.fetchall()]
            
            seen_hashes = set()
            dropped_tables = []
            
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*), MIN(id), MAX(id) FROM {table}")
                    stats = cursor.fetchone()
                    if not stats or stats[0] == 0:
                        # Удаляем пустые таблицы
                        cursor.execute(f"DROP TABLE {table}")
                        cursor.execute(f"DROP TABLE IF EXISTS {table}_fts")
                        cursor.execute(f"DROP TABLE IF EXISTS {table}_fts_data")
                        cursor.execute(f"DROP TABLE IF EXISTS {table}_fts_idx")
                        cursor.execute(f"DROP TABLE IF EXISTS {table}_fts_docsize")
                        cursor.execute(f"DROP TABLE IF EXISTS {table}_fts_config")
                        dropped_tables.append(f"{table} (пустая)")
                        continue
                        
                    cursor.execute(f"SELECT DISTINCT chat_id FROM {table} LIMIT 5")
                    chat_ids = tuple([row[0] for row in cursor.fetchall()])
                    
                    table_hash = (stats[0], stats[1], stats[2], chat_ids)
                    if table_hash in seen_hashes:
                        # Нашли точный дубликат! Удаляем.
                        cursor.execute(f"DROP TABLE {table}")
                        cursor.execute(f"DROP TABLE IF EXISTS {table}_fts")
                        cursor.execute(f"DROP TABLE IF EXISTS {table}_fts_data")
                        cursor.execute(f"DROP TABLE IF EXISTS {table}_fts_idx")
                        cursor.execute(f"DROP TABLE IF EXISTS {table}_fts_docsize")
                        cursor.execute(f"DROP TABLE IF EXISTS {table}_fts_config")
                        dropped_tables.append(table)
                    else:
                        seen_hashes.add(table_hash)
                except sqlite3.OperationalError:
                    continue # Пропускаем таблицы с другой структурой

            conn.commit()
            
        if dropped_tables:
            return jsonify({'success': f'Успешно удалено {len(dropped_tables)} дублированных таблиц: {", ".join(dropped_tables)}'})
        else:
            return jsonify({'success': 'Дублированных таблиц не найдено. База в порядке!'})
            
    except Exception as e:
        return jsonify({'error': f'Ошибка при удалении дубликатов: {e}'})

def extract_table_name(sql_query):
    match = re.search(r'FROM\s+(\w+)', sql_query, re.IGNORECASE)
    return match.group(1) if match else 'default_table'

@app.route('/details', methods=['GET'])
def details():
    id = request.args.get('id')
    table_name = request.args.get('my_table', 'default_table')  # Provide a default table name if not specified
    if id is not None:
        query = f"SELECT * FROM {table_name} WHERE ID > {int(id) - 200} AND ID <= {int(id) + 200}"
        result_data = db.execute_query(query)
        return render_template('messages.html', results=result_data, selected_id=id)

    return render_template('messages.html', results=[])

@app.route('/search_page', methods=['GET', 'POST'])
def search_page():
    if request.method == 'POST':
        search_text = request.form['search_text']
        search_results = search_all_tables(search_text)
        return render_template('search_page.html', search_results=search_results, search_text=search_text)
    
    return render_template('search_page.html', search_results=None)
def search_all_tables(search_text):
    tables = get_table_list()  # Получаем список всех таблиц
    search_results = []

    for table in tables:
        query = f"SELECT * FROM {table} WHERE message_text LIKE ?"
        with sqlite3.connect(DATABASE) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (search_text,))
            results = cursor.fetchall()
            if results:
                search_results.append({'table': table, 'results': results})

    return search_results

@app.route('/search_page_fts', methods=['GET', 'POST'])
def search_page_fts():
    tables = get_table_list()
    from datetime import datetime
    if request.method == 'POST':
        search_text = request.form.get('search_text', '').strip()
        selected_table = request.form.get('selected_table', 'all')
        chat_id = request.form.get('chat_id', '').strip()
        author_name = request.form.get('author_name', '').strip()
        has_attachments = request.form.get('has_attachments') == 'on'
        sort_order = request.form.get('sort_order', 'DESC')
        current_limit = int(request.form.get('current_limit', 200))

        if selected_table == 'all' or not selected_table:
            tables_to_search = tables
        elif selected_table in tables:
            tables_to_search = [selected_table]
        else:
            tables_to_search = []
            
        flattened_metadata = []
        
        def parse_date_to_tuple(date_str):
            try:
                parts = date_str.split(' ')
                d = parts[0]
                t = parts[1] if len(parts) > 1 else '00:00'
                
                d_parts = d.split('.')
                day = int(d_parts[0])
                month = int(d_parts[1])
                year = int(d_parts[2])
                
                t_parts = t.split(':')
                hh = int(t_parts[0])
                mm = int(t_parts[1])
                return (year, month, day, hh, mm)
            except Exception:
                return (0, 0, 0, 0, 0)
                
        use_pymorphy = request.form.get('use_pymorphy') == 'on'
        use_pagination = request.form.get('use_pagination') == 'on'

        if not use_pagination and not search_text and selected_table == 'all':
            # Защита сервера от OOM: запрещаем отключать лимиты при пустом поиске по всем таблицам
            use_pagination = True

        search_lemmatized = ""
        if search_text:
            if use_pymorphy:
                from parser_service import lemmatize_text
                search_lemmatized = lemmatize_text(search_text)
                
        import time
        import threading
        
        total_tables = len(tables_to_search)
        tables_processed = [0]
        print_lock = threading.Lock()
        
        print(f"\n[{time.strftime('%H:%M:%S')}] Начинаю масштабированный поиск. Целей: {total_tables} таблиц. (Словарь Pymorphy3: {'ВКЛ' if use_pymorphy else 'ВЫКЛ'})")

        def process_tables_chunk(tables_chunk):
            local_results = []
            try:
                with sqlite3.connect(DATABASE, timeout=30.0) as local_conn:
                    local_cursor = local_conn.cursor()
                    for table in tables_chunk:
                        params = []
                        
                        if search_text:
                            base_query = f"SELECT {table}.id, {table}.message_date FROM {table} INNER JOIN {table}_fts ON {table}.id = {table}_fts.rowid WHERE {table}_fts MATCH ?"
                            if use_pymorphy:
                                params.append(f'message_text_lemma:("{search_lemmatized}")')
                            else:
                                params.append(f'message_text_raw:("{search_text}"*)')
                            order_column = f"{table}_fts.rowid"
                        else:
                            base_query = f"SELECT {table}.id, {table}.message_date FROM {table} WHERE 1=1"
                            order_column = f"{table}.rowid"
                            
                        if chat_id:
                            base_query += f" AND {table}.chat_id = ?"
                            params.append(chat_id)
                            
                        if author_name:
                            base_query += f" AND {table}.author_name LIKE ?"
                            params.append(f"%{author_name}%")
                            
                        if has_attachments:
                            base_query += f" AND {table}.attachment_links != '[]' AND {table}.attachment_links IS NOT NULL"
                            
                        order_dir = "ASC" if sort_order == "ASC" else "DESC"
                        if use_pagination:
                            base_query += f" ORDER BY {order_column} {order_dir} LIMIT ?"
                            params.append(current_limit + 1)
                        else:
                            base_query += f" ORDER BY {order_column} {order_dir}"
                        
                        try:
                            local_cursor.execute(base_query, params)
                            results = local_cursor.fetchall()
                            for r in results:
                                date_tuple = parse_date_to_tuple(str(r[1]).strip())
                                local_results.append((date_tuple, table, r[0]))
                        except Exception as e:
                            print(f"Ошибка поиска (часть 1) в {table}: {e}")
                            
                        with print_lock:
                            tables_processed[0] += 1
                            if tables_processed[0] % 100 == 0 or tables_processed[0] == total_tables:
                                print(f"[{time.strftime('%H:%M:%S')}] ... Просканировано {tables_processed[0]}/{total_tables} таблиц")
            except Exception as e:
                print(f"Сбой подключения чанка: {e}")
                
            return local_results

        flattened_metadata = []
        import math
        chunk_size = math.ceil(len(tables_to_search) / 32)
        if chunk_size == 0: chunk_size = 1
        chunks = [tables_to_search[i:i + chunk_size] for i in range(0, len(tables_to_search), chunk_size)]
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=16) as executor:
            future_to_chunk = {executor.submit(process_tables_chunk, chunk): chunk for chunk in chunks}
            for future in as_completed(future_to_chunk):
                res = future.result()
                if res:
                    flattened_metadata.extend(res)

        if selected_table != 'all':
            flattened_metadata.sort(key=lambda x: x[0], reverse=(sort_order == 'DESC'))
        else:
            # По просьбе переключаем: если "Все таблицы", даты глобально не сортируются (результаты группируются блоками по диалогам)
            flattened_metadata.sort(key=lambda x: x[1])
        
        if use_pagination:
            has_more = len(flattened_metadata) > current_limit
            paginated_meta = flattened_metadata[:current_limit]
            total_results = current_limit if has_more else len(paginated_meta)
        else:
            has_more = False
            paginated_meta = flattened_metadata
            total_results = len(paginated_meta)
        
        paginated_results = [None] * len(paginated_meta)
        table_to_ids = {}
        for idx, item in enumerate(paginated_meta):
            table_to_ids.setdefault(item[1], []).append((item[2], idx))
            
        total_detail_fetches = len(table_to_ids)
        if total_detail_fetches > 0:
            print(f"[{time.strftime('%H:%M:%S')}] Подготовка результатов. Скачиваю развернутые детали из {total_detail_fetches} таблиц...")
            
        fetches_done = 0
        with sqlite3.connect(DATABASE) as conn:
            cursor = conn.cursor()
            for table, ids_info in table_to_ids.items():
                ids_str = ','.join(str(x[0]) for x in ids_info)
                try:
                    cursor.execute(f"SELECT * FROM {table} WHERE id IN ({ids_str})")
                    full_rows = {row[0]: row for row in cursor.fetchall()}
                    for row_id, global_idx in ids_info:
                        if row_id in full_rows:
                            paginated_results[global_idx] = {'table': table, 'row': full_rows[row_id]}
                except Exception as e:
                    print(f"Ошибка загрузки деталей (часть 2) в {table}: {e}")
                    
                fetches_done += 1
                if fetches_done % 50 == 0 or fetches_done == total_detail_fetches:
                    print(f"[{time.strftime('%H:%M:%S')}] ... Детали загружены ({fetches_done}/{total_detail_fetches})")
                    
        paginated_results = [x for x in paginated_results if x is not None]

        print(f"[{time.strftime('%H:%M:%S')}] Выдача поиска сформирована!")

        return render_template('search_page_fts.html', search_results=paginated_results, tables=tables, 
                               search_text=search_text, selected_table=selected_table, 
                               chat_id=chat_id, author_name=author_name, has_attachments=has_attachments, sort_order=sort_order,
                               current_limit=current_limit, has_more=has_more, total_results=total_results,
                               use_pymorphy=use_pymorphy, use_pagination=use_pagination)
    
    return render_template('search_page_fts.html', search_results=None, tables=tables, use_pagination=True)


def get_table_list():
    with sqlite3.connect(DATABASE) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
    exclude_suffixes = ('_fts', '_fts_data', '_fts_idx', '_fts_content', '_fts_docsize', '_fts_config')
    return [table[0] for table in tables if not table[0].endswith(exclude_suffixes) and table[0] != 'sqlite_sequence']


def setup_fts_for_existing_tables():
    import time
    with sqlite3.connect(DATABASE, timeout=60.0) as conn:
        try:
            conn.execute('PRAGMA journal_mode=WAL;')
        except sqlite3.OperationalError:
            pass # Игнорируем блокировку: если БД занята другим процессом (например Flask reloader), WAL режим скорее всего уже активирован ранее.
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        all_db_tables = [t[0] for t in cursor.fetchall()]
        
    valid_tables = get_table_list()
    total_start = time.time()
    with sqlite3.connect(DATABASE, timeout=60.0) as conn:
        cursor = conn.cursor()
        for table in valid_tables:
            fts_table = f"{table}_fts"
            is_old = False
            if fts_table in all_db_tables:
                cursor.execute("SELECT sql FROM sqlite_master WHERE name=?", (fts_table,))
                sql_row = cursor.fetchone()
                if sql_row and sql_row[0]:
                    sql = sql_row[0].lower()
                    if "message_text_raw" not in sql or "message_text_lemma" not in sql:
                        is_old = True
                        print(f"[{time.strftime('%H:%M:%S')}] Dropping obsolete FTS index for {table}...")
                        cursor.execute(f"DROP TABLE {fts_table}")
                    else:
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM {fts_table}")
                            fts_count = cursor.fetchone()[0]
                            
                            cursor.execute(f"SELECT COUNT(*) FROM {table}")
                            main_count = cursor.fetchone()[0]
                            
                            if fts_count == 0 and main_count > 0:
                                is_old = True
                                print(f"[{time.strftime('%H:%M:%S')}] Обнаружена пустая FTS таблица {fts_table} (вероятно после обрыва)! Пересоздаю...")
                                cursor.execute(f"DROP TABLE {fts_table}")
                        except Exception as e:
                            pass
                    
            if fts_table not in all_db_tables or is_old:
                t_start = time.time()
                print(f"[{time.strftime('%H:%M:%S')}] Начало создания лемматизированного FTS индекса для {table}...")
                cursor.execute(f'''
                    CREATE VIRTUAL TABLE IF NOT EXISTS {fts_table} USING fts5(
                        message_text_raw,
                        message_text_lemma
                    )
                ''')
                
                cursor.execute(f"SELECT id, message_text FROM {table}")
                rows = cursor.fetchall()
                if rows:
                    from concurrent.futures import ProcessPoolExecutor, as_completed
                    from parser_service import lemmatize_chunk
                    import os
                    
                    chunk_size = 10000
                    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]
                    fts_data = []
                    total_rows = len(rows)
                    processed = 0
                    
                    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
                        futures = {executor.submit(lemmatize_chunk, chunk): chunk for chunk in chunks}
                        for future in as_completed(futures):
                            try:
                                res = future.result()
                                fts_data.extend(res)
                                processed += len(res)
                                print(f"  ... лемматизировано {processed}/{total_rows} строк ({(processed/total_rows*100):.1f}%, {(time.time()-t_start):.1f} сек)")
                            except Exception as e:
                                print(f"Ошибка при параллельной лемматизации: {e}")
                        
                    cursor.executemany(f"INSERT OR REPLACE INTO {fts_table}(rowid, message_text_raw, message_text_lemma) VALUES (?, ?, ?)", fts_data)
                print(f"[{time.strftime('%H:%M:%S')}] Индексация {table} завершена за {time.time()-t_start:.2f} сек. (строк: {len(rows) if rows else 0})")
        conn.commit()
    print(f"[{time.strftime('%H:%M:%S')}] Глобальное обновление FTS индексов завершено за {time.time()-total_start:.2f} сек.")

import multiprocessing

if __name__ == '__main__':
    print("🚀 Сервер запущен. FTS-индексация теперь запускается вручную из интерфейса!")
    app.run(debug=True, use_reloader=False)
