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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
    
    def parse_date_ft(self, date_str):
        # FT duplica la fecha: "Wednesday, February 11, 2026Wed, Feb 11, 2026"
        # Separamos por las abreviaturas de días
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
        
        # Si se separó, usar la parte corta (después del split)
        if len(date_parts) > 1:
            date_clean = date_parts[1].strip()
        else:
            date_clean = date_str.strip()
        
        # Intentar parsear formato corto: "Feb 11, 2026"
        formats = ["%b %d, %Y", "%B %d, %Y"]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_clean, fmt)
                return dt.strftime("%Y-%m-%d")
            except:
                continue
        return None
    
    def scrape(self, isin):
        url = self.BASE_URL.format(isin=isin)
        logger.info(f"FT: Scrapeando {url}")
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            logger.info(f"FT: Status code {response.status_code}")
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            name_elem = soup.find('h1', class_='mod-tearsheet-overview__header__name')
            if not name_elem:
                name_elem = soup.find('h1')
            fund_name = name_elem.get_text(strip=True) if name_elem else ""
            
            logger.info(f"FT: Nombre del fondo: '{fund_name}'")
            
            table = soup.find('table', class_='mod-tearsheet-historical-prices__results')
            
            if not table:
                logger.warning(f"FT: No se encontró tabla con clase 'mod-tearsheet-historical-prices__results'")
                all_tables = soup.find_all('table')
                logger.info(f"FT: Se encontraron {len(all_tables)} tablas en total")
                
                if all_tables:
                    logger.info(f"FT: Intentando usar la primera tabla disponible")
                    table = all_tables[0]
                else:
                    return {"name": fund_name, "currency": "EUR", "prices": []}
            
            tbody = table.find('tbody')
            if not tbody:
                logger.warning(f"FT: No se encontró tbody, usando tr directamente")
                rows = table.find_all('tr')[1:]
            else:
                rows = tbody.find_all('tr')
            
            logger.info(f"FT: Filas encontradas: {len(rows)}")
            
            prices = []
            for idx, row in enumerate(rows):
                cols = row.find_all('td')
                
                if len(cols) < 5:
                    logger.debug(f"FT: Fila {idx} tiene solo {len(cols)} columnas, saltando")
                    continue
                
                date_raw = cols[0].get_text(strip=True)
                close_price_raw = cols[4].get_text(strip=True)
                
                date_parsed = self.parse_date_ft(date_raw)
                if not date_parsed:
                    logger.debug(f"FT: No se pudo parsear fecha '{date_raw}'")
                    continue
                
                try:
                    price_clean = close_price_raw.replace(',', '').strip()
                    
                    if not price_clean or price_clean in ['0', '0.00', '0.0', '00.00']:
                        logger.debug(f"FT: Precio inválido '{price_clean}', saltando")
                        continue
                    
                    price_value = float(price_clean)
                    
                    prices.append({
                        "date": date_parsed,
                        "price": price_value,
                        "source": "ft",
                        "priority": 20
                    })
                    
                    logger.debug(f"FT: Precio añadido: {date_parsed} = {price_value}")
                    
                except (ValueError, IndexError) as e:
                    logger.debug(f"FT: Error parseando precio '{close_price_raw}': {e}")
                    continue
            
            logger.info(f"FT: {len(prices)} precios obtenidos para {isin}")
            
            if len(prices) == 0:
                logger.warning(f"FT: ADVERTENCIA - No se obtuvieron precios. Revisa logs de debug.")
            
            return {"name": fund_name, "currency": "EUR", "prices": prices}
        
        except requests.exceptions.RequestException as e:
            logger.error(f"FT: Error de conexión para {isin}: {str(e)}")
            return {"name": "", "currency": "EUR", "prices": []}
        except Exception as e:
            logger.error(f"FT: Error inesperado para {isin}: {str(e)}")
            import traceback
            logger.error(f"FT: Traceback: {traceback.format_exc()}")
            return {"name": "", "currency": "EUR", "prices": []}
