import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sys
from pathlib import Path
import re
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.logger import setup_logger

logger = setup_logger(__name__)

class FundsquareScraper:
    BASE_URL = "https://www.fundsquare.net/security/histo-prices"
    
    def __init__(self, timeout=30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def parse_date_fs(self, date_str):
        try:
            dt = datetime.strptime(date_str, "%d/%m/%Y")
            return dt.strftime("%Y-%m-%d")
        except:
            return None
    
    def scrape_page(self, id_instr, page=1):
        url = f"{self.BASE_URL}?idInstr={id_instr}&page={page}"
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            table = soup.find('table', class_='tabHorizontal')
            if not table:
                return [], 0
            
            # Detectar número total de páginas
            total_pages = 1
            pagination_text = soup.find('p', string=lambda x: x and 'Number of pages' in x)
            if pagination_text:
                match = re.search(r'Number of pages[:\s]+(\d+)', pagination_text.get_text())
                if match:
                    total_pages = int(match.group(1))
            
            prices = []
            rows = table.find_all('tr')[2:]
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 2:
                    continue
                
                date_raw = cols[0].get_text(strip=True)
                price_raw = cols[1].get_text(strip=True)
                
                date_parsed = self.parse_date_fs(date_raw)
                if not date_parsed:
                    continue
                
                try:
                    price_part = price_raw.split()[0]
                    price_clean = price_part.replace(',', '.').replace(' ', '')
                    price_value = float(price_clean)
                    
                    prices.append({
                        "date": date_parsed,
                        "price": price_value,
                        "source": "fundsquare",
                        "priority": 10
                    })
                except (ValueError, IndexError):
                    continue
            
            return prices, total_pages
        
        except Exception as e:
            logger.error(f"Fundsquare: Error en página {page}: {str(e)}")
            return [], 0
    
    def scrape(self, id_instr, max_pages=50):
        logger.info(f"Fundsquare: https://www.fundsquare.net/security/histo-prices?idInstr={id_instr}")
        
        all_prices = []
        
        # Primera página para detectar total
        prices, total_pages = self.scrape_page(id_instr, page=1)
        all_prices.extend(prices)
        
        logger.info(f"Fundsquare: {len(prices)} precios en página 1 de {total_pages}")
        
        # Limitar a max_pages para evitar timeouts
        pages_to_fetch = min(total_pages, max_pages)
        
        # Resto de páginas
        for page in range(2, pages_to_fetch + 1):
            time.sleep(0.5)  # Delay para evitar bloqueos
            prices, _ = self.scrape_page(id_instr, page=page)
            all_prices.extend(prices)
            logger.info(f"Fundsquare: {len(prices)} precios en página {page}")
        
        logger.info(f"Fundsquare: {len(all_prices)} precios totales obtenidos")
        return {"prices": all_prices}
