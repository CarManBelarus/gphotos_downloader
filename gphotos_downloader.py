import asyncio
import aiohttp
import aiofiles
import re
import json
import os
import urllib.parse
import time
from tqdm.asyncio import tqdm

# Глабальныя налады
CONCURRENT_DOWNLOADS = 3  
CHUNK_SIZE = 1024 * 1024  
MAX_RETRIES = 5           

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate" 
}

class GooglePhotosAlbumDownloader:
    def __init__(self, album_url, output_dir):
        self.album_url = album_url
        self.output_dir = output_dir
        self.canonical_url = ""
        self.album_id = ""
        self.auth_key = ""
        self.photo_urls = set()
        
        # Зменныя для разліку MB/s
        self.start_time = 0
        self.downloaded_bytes = 0
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    async def run(self):
        print("[*] Ініцыялізацыя сесіі і апрацоўка перанакіраванняў (Фаза 1)...")
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            html = await self._fetch_initial_html(session)
            print(f"[*] ID альбома: {self.album_id} | Ключ: {self.auth_key}")
            
            print("[*] Пошук медыяфайлаў на старонцы (Фаза 2)...")
            self._extract_urls(html)
            continuation_token = self._find_token(html)
            
            if continuation_token:
                print("[*] Альбом вялікі. Запуск перагортвання старонак (Фаза 3)...")
                await self._batchexecute_loop(session, continuation_token)
            else:
                print("[*] Альбом цалкам змясціўся на першай старонцы.")

            if not self.photo_urls:
                print("[-] ПАМЫЛКА: Не знойдзена ніводнай спасылкі для спампоўвання.")
                return

            print(f"[*] Усяго сабрана ўнікальных файлаў: {len(self.photo_urls)}")
            print("[*] Запуск спампоўвання...")
            await self._download_all_files(session)

    async def _fetch_initial_html(self, session):
        async with session.get(self.album_url, allow_redirects=True) as response:
            response.raise_for_status()
            self.canonical_url = str(response.url)
            parsed_url = urllib.parse.urlparse(self.canonical_url)
            path_parts = parsed_url.path.split('/')
            
            if len(path_parts) > 2 and path_parts[1] == "share":
                self.album_id = path_parts[2]
            
            query_params = urllib.parse.parse_qs(parsed_url.query)
            if 'key' in query_params:
                self.auth_key = query_params['key'][0]
                
            return await response.text()

    def _extract_urls(self, text):
        # Ачышчаем тэкст ад экранаваных слэшаў, каб рэгулярны выраз працаваў ідэальна
        clean_text = text.replace('\\/', '/')
        raw_urls = re.findall(r'(https://lh[0-9]\.googleusercontent\.com/[a-zA-Z0-9\-_/]+)', clean_text)
        for u in raw_urls:
            if "/a/" not in u and "/a-/" not in u and len(u.split('/')[-1]) > 25:
                self.photo_urls.add(u)

    def _find_token(self, text, current_token=None):
        # Ачышчаем тэкст
        clean_text = text.replace('\\/', '/').replace('\\"', '"')
        possible_tokens = re.findall(r'"([A-Za-z0-9\-_]{60,})"', clean_text)
        
        # Адкідваем ID фатаграфій
        valid_tokens = [t for t in possible_tokens if not t.startswith("AF1Q")]
        
        if valid_tokens:
            # САКРЭТ ВЫПРАЎЛЕННЯ: Токен пагінацыі заўсёды знаходзіцца ў самым канцы адказу Google!
            # Таму мы бярэм АПОШНІ валідны токен са знойдзеных
            for token in reversed(valid_tokens):
                if token != current_token:
                    return token
        return None

    async def _batchexecute_loop(self, session, token):
        rpc_url = "https://photos.google.com/_/PhotosUi/data/batchexecute"
        page_num = 1
        
        while token:
            rpc_args = json.dumps([self.album_id, token, None, self.auth_key, None])
            f_req = json.dumps([[["snAcKc", rpc_args, None, "generic"]]])
            payload = {'f.req': f_req}
            
            async with session.post(rpc_url, data=payload) as response:
                response.raise_for_status()
                raw_text = await response.text()
                clean_text = raw_text.lstrip(")]}'\n ")
                
                try:
                    data = json.loads(clean_text)
                    # Выцягваем новыя URL
                    self._extract_urls(clean_text) 
                    # Шукаем токен для НАСТУПНАЙ старонкі
                    new_token = self._find_token(clean_text, current_token=token)
                    
                    if new_token == token:
                        break # Абарона ад бясконцага цыклу
                    token = new_token
                    
                    page_num += 1
                    print(f"    -> Апрацавана схаваная старонка {page_num} (усяго файлаў у памяці: {len(self.photo_urls)})")
                except json.JSONDecodeError:
                    break

    async def _download_all_files(self, session):
        semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)
        tasks = []
        
        self.start_time = time.time()
        
        # Прагрэс-бар наладжаны ВЫКЛЮЧНА на колькасць файлаў (штукі)
        with tqdm(total=len(self.photo_urls), desc="Спампоўванне", unit="файл") as pbar:
            for base_url in self.photo_urls:
                clean_base = base_url.split('=')[0]
                task = asyncio.create_task(self._download_single_file(session, clean_base, semaphore, pbar))
                tasks.append(task)
                
            await asyncio.gather(*tasks)

    async def _download_single_file(self, session, base_url, semaphore, pbar):
        async with semaphore:
            for attempt in range(MAX_RETRIES):
                try:
                    # Спрабуем як відэа
                    video_url = f"{base_url}=dv"
                    async with session.get(video_url) as resp:
                        if resp.status == 200:
                            ctype = resp.headers.get('Content-Type', '').lower()
                            cdisp = resp.headers.get('Content-Disposition', '').lower()
                            if 'video/' in ctype or 'octet-stream' in ctype or any(ext in cdisp for ext in ['.mts', '.mp4', '.mov', '.mkv', '.avi']):
                                success = await self._save_stream(resp, video_url, pbar)
                                if success: pbar.update(1) # Дадаём 1 спампаваны файл
                                return success
                    
                    # Спрабуем як фота
                    photo_url = f"{base_url}=d"
                    async with session.get(photo_url) as resp:
                        resp.raise_for_status()
                        success = await self._save_stream(resp, photo_url, pbar)
                        if success: pbar.update(1) # Дадаём 1 спампаваны файл
                        return success
                        
                except Exception as e:
                    wait_time = 2 ** attempt
                    await asyncio.sleep(wait_time)
            
            print(f"\n[-] Памылка спампоўвання: {base_url}")
            pbar.update(1) # Прапускаем файл з памылкай, каб прагрэс-бар не завіс
            return False

    async def _save_stream(self, response, url, pbar):
        content_disposition = response.headers.get('Content-Disposition', '')
        filename_match = re.search(r'filename="([^"]+)"', content_disposition)
        
        if filename_match:
            filename = filename_match.group(1)
        else:
            clean_hash = url.split('/')[-1].split('=')[0][:12]
            ext = ".mp4" if "=dv" in url else ".jpg"
            filename = f"media_{clean_hash}{ext}"
            
        filepath = os.path.join(self.output_dir, filename)
        file_size = int(response.headers.get('Content-Length', 0))
            
        if os.path.exists(filepath):
            local_size = os.path.getsize(filepath)
            # Файл ужо цалкам спампаваны
            if file_size > 0 and local_size == file_size:
                return True
            
        async with aiofiles.open(filepath, mode='wb') as f:
            async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                await f.write(chunk)
                
                # Разлік хуткасці "на ляту"
                self.downloaded_bytes += len(chunk)
                elapsed = time.time() - self.start_time
                if elapsed > 0:
                    speed = (self.downloaded_bytes / 1024 / 1024) / elapsed
                    # Выводзім хуткасць тэкстам побач з прагрэс-барам
                    pbar.set_postfix_str(f"Хуткасць: {speed:.1f} MB/s", refresh=False)
                
        return True

if __name__ == "__main__":
    ALBUM_LINK = input("Увядзіце публічную спасылку на альбом: ").strip()
    OUTPUT_FOLDER = input("Увядзіце назву папкі: ").strip()
    
    downloader = GooglePhotosAlbumDownloader(ALBUM_LINK, OUTPUT_FOLDER)
    
    try:
        asyncio.run(downloader.run())
        print(f"\n[+] Паспяхова! Файлы ляжаць у папцы '{OUTPUT_FOLDER}'.")
    except KeyboardInterrupt:
        print("\n[-] Працэс перарваны.")
    except Exception as ex:
        print(f"\n[-] Крытычная памылка: {ex}")
