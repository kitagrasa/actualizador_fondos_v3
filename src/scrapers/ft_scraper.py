import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sys
from pathlib import Path

# Añadir directorio raíz al path
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
        """
        Convierte formatos FT a ISO:
        'Wednesday, September 24, 2025' -> '2025-09-24'
        'Wed, Sep 24, 2025' -> '2025-09-24'
        """
        # Limpiar sufijos de día
        date_clean = date_str.split('Wed, ')[-1].split('Thu, ')[-1].split('Fri, ')[-1]
        date_clean = date_clean.split('Mon, ')[-1].split('Tue, ')[-1].split('Sat, ')[-1].split('Sun, ')[-1]
        
        # Probar formatos
        formats = [
            "%A, %B %d, %Y",      # Wednesday, September 24, 2025
            "%b %d, %Y",          # Sep 24, 2025
            "%B %d, %Y"           # September 24, 2025
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_clean.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except:
                continue
        return None
    
    def scrape(self, isin):
        """
        Devuelve:
        {
            "name": "Bestinver Tordesillas...",
            "currency": "EUR",
            "prices": [
                {"date": "2025-09-24", "price": 27.32, "source": "ft", "priority": 20},
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
            if not name_elem:
                # Intentar alternativa
                name_elem = soup.find('h1')
            fund_name = name_elem.get_text(strip=True) if name_elem else ""
            
            # Buscar tabla de precios históricos (múltiples intentos)
            table = (
                soup.find('table', class_='mod-tearsheet-historical-prices__results') or
                soup.find('table', class_='mod-ui-table') or
                soup.find('table')
            )
            
            if not table:
                logger.warning(f"⚠️  FT: Tabla no encontrada para {isin}")
                return {"name": fund_name, "currency": "EUR", "prices": []}
            
            prices = []
            rows = table.find_all('tr')[1:]  # Saltar header
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 2:
                    continue
                
                # Columna fecha (primera)
                date_raw = cols[0].get_text(strip=True)
                date_parsed = self.parse_date_ft(date_raw)
                
                if not date_parsed:
                    continue
                
                # Buscar precio en columna "Close" (típicamente columna 4, índice 3)
                # Pero si no hay 5 columnas, usar la segunda
                price_col_idx = 3 if len(cols) >= 5 else 1
                price_raw = cols[price_col_idx].get_text(strip=True)
                
                try:
                    # Limpiar precio: "27.32" o "1,234.56"
                    price_clean = price_raw.replace(',', '').replace('€', '').replace('EUR', '').strip()
                    
                    # Ignor
