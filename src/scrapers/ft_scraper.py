import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import sys
from pathlib import Path
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.logger import setup_logger

logger = setup_logger(__name__)

class FTScraper:
    BASE_URL = "https://markets.ft.com/data/funds/tearsheet/historical?s={isin}:EUR"
    
    def __init__(self, timeout=30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        })
    
    def parse_date_ft(self, date_str):
        date_parts = date_str.split('Mon, ')
        if len(date_parts) == 1:
            date_parts = date_str.split('Tue, ')
        if len(date_parts) == 1:
            date_parts = date_str.split('Wed, ')
        if len(date_parts) == 1:
            date_parts = date_str.split('Thu, ')
        if len(date_parts) == 1:
            date_parts = date_str.split('Fri, ')
        if len(date_parts) == 1:
            date_parts = date_str.split('Sat, ')
        if len(date_parts) == 1:
            date_parts = date_str.split('Sun, ')
        
        if len(date_parts) > 1:
            date_clean = date_parts[1].strip()
        else:
            date_clean = date_str.strip()
        
        formats = ["%b %d, %Y", "%B %d, %Y"]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_clean, fmt)
                return dt.strftime("%Y-%m-%d")
            except:
                continue
        return None
    
    def scrape_single_page(self, url):
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            table = soup.find('table', class_='mod-tearsheet-historical-prices__results')
            if not table:
                return []
            
            tbody = table.find('tbody')
            if not tbody:
                rows = table.find_all('tr')[1:]
            else:
                rows = tbody.find_all('tr')
            
            prices = []
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
                    
                    if not price_clean or price_clean in ['0', '0.00', '0.0', '00.00']:
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
            
            return prices
        
        except Exception as e:
            logger.error(f"FT: Error en request: {str(e)}")
            return []
    
    def scrape(self, isin, years_back=20):
        logger.info(f"FT: Scrapeando hasta {years_back} años para {isin}")
        
        all_prices = []
        fund_name = ""
        
        end_date = datetime.now()
        
        for year in range(years_back):
            start_date = end_date - timedelta(days=365)
            
            url = f"{self.BASE_URL.format(isin=isin)}&startDate={start_date.strftime('%Y%m%d')}&endDate={end_date.strftime('%Y%m%d')}"
            
            logger.debug(f"FT: Rango {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
            
            prices = self.scrape_single_page(url)
            all_prices.extend(prices)
            
            logger.debug(f"FT: {len(prices)} precios en este rango")
            
            if len(prices) == 0 and year > 0:
                logger.info(f"FT: Sin datos más antiguos, deteniendo en año {year}")
                break
            
            end_date = start_date - timedelta(days=1)
            
            time.sleep(0.5)
        
        try:
            response = self.session.get(self.BASE_URL.format(isin=isin), timeout=self.timeout)
            soup = BeautifulSoup(response.text, 'lxml')
            name_elem = soup.find('h1', class_='mod-tearsheet-overview__header__name')
            if not name_elem:
                name_elem = soup.find('h1')
            fund_name = name_elem.get_text(strip=True) if name_elem else ""
        except:
            fund_name = ""
        
        logger.info(f"FT: {len(all_prices)} precios totales obtenidos para {isin}")
        
        return {"name": fund_name, "currency": "EUR", "prices": all_prices}
