import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.logger import setup_logger

logger = setup_logger(__name__)

class FTScraper:
    BASE_URL = "https://markets.ft.com/data/funds/tearsheet/historical?s={isin}:EUR"
    
    def __init__(self, timeout=30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def parse_date_ft(self, date_str):
        date_clean = date_str.strip()
        
        for prefix in ['Mon, ', 'Tue, ', 'Wed, ', 'Thu, ', 'Fri, ', 'Sat, ', 'Sun, ']:
            date_clean = date_clean.replace(prefix, '')
        
        formats = [
            "%A, %B %d, %Y",
            "%b %d, %Y",
            "%B %d, %Y"
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_clean.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except:
                continue
        return None
    
    def scrape(self, isin):
        url = self.BASE_URL.format(isin=isin)
        logger.info(f"FT: {url}")
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            name_elem = soup.find('h1', class_='mod-tearsheet-overview__header__name')
            if not name_elem:
                name_elem = soup.find('h1')
            fund_name = name_elem.get_text(strip=True) if name_elem else ""
            
            table = soup.find('table', class_='mod-tearsheet-historical-prices__results')
            
            if not table:
                logger.warning(f"FT: Tabla no encontrada para {isin}")
                return {"name": fund_name, "currency": "EUR", "prices": []}
            
            prices = []
            rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 5:
                    continue
                
                date_raw = cols[0].get_text(strip=True)
                close_price_raw = cols[4].get_text(strip=True)
                
                date_parsed = self.parse_date_ft(date_raw)
                if not date_parsed:
                    continue
                
                try:
                    price_clean = close_price_raw.replace(',', '').strip()
                    
                    if not price_clean or price_clean in ['0', '0.00', '0.0']:
                        continue
                    
                    price_value = float(price_clean)
                    
                    prices.append({
                        "date": date_parsed,
                        "price": price_value,
                        "source": "ft",
                        "priority": 20
                    })
                except (ValueError, IndexError):
                    continue
            
            logger.info(f"FT: {len(prices)} precios obtenidos para {isin}")
            return {"name": fund_name, "currency": "EUR", "prices": prices}
        
        except Exception as e:
            logger.error(f"FT Error para {isin}: {str(e)}")
            return {"name": "", "currency": "EUR", "prices": []}
