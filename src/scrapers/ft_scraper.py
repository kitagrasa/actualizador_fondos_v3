import requests
from bs4 import BeautifulSoup
from datetime import datetime
from ..utils.logger import setup_logger

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
        """Convierte 'Monday, February 10, 2026' a '2026-02-10'"""
        try:
            dt = datetime.strptime(date_str, "%A, %B %d, %Y")
            return dt.strftime("%Y-%m-%d")
        except:
            return None
    
    def scrape(self, isin):
        """
        Devuelve:
        {
            "name": "Bestinver Tordesillas...",
            "currency": "EUR",
            "prices": [
                {"date": "2026-02-10", "price": 32.828, "source": "ft", "priority": 20},
                ...
            ]
        }
        """
        url = self.BASE_URL.format(isin=isin)
        logger.info(f"🌐 FT: {url}")
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Extraer nombre del fondo
            name_elem = soup.find('h1', class_='mod-tearsheet-overview__header__name')
            fund_name = name_elem.get_text(strip=True) if name_elem else ""
            
            # Tabla de precios históricos
            table = soup.find('table', class_='mod-tearsheet-historical-prices__results')
            if not table:
                logger.warning(f"⚠️  FT: Tabla no encontrada para {isin}")
                return {"name": fund_name, "currency": "EUR", "prices": []}
            
            prices = []
            rows = table.find_all('tr')[1:]  # Saltar header
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    date_raw = cols[0].get_text(strip=True)
                    price_raw = cols[1].get_text(strip=True)
                    
                    date_parsed = self.parse_date_ft(date_raw)
                    if not date_parsed:
                        continue
                    
                    try:
                        price_clean = price_raw.replace(',', '')
                        price_value = float(price_clean)
                        
                        prices.append({
                            "date": date_parsed,
                            "price": price_value,
                            "source": "ft",
                            "priority": 20
                        })
                    except ValueError:
                        continue
            
            logger.info(f"✓ FT: {len(prices)} precios obtenidos")
            return {"name": fund_name, "currency": "EUR", "prices": prices}
        
        except Exception as e:
            logger.error(f"❌ FT Error para {isin}: {e}")
            return {"name": "", "currency": "EUR", "prices": []}
