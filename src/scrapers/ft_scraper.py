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
            'Connection': 'keep-alive'
        })
    
    def parse_date_ft(self, date_str):
        """
        Parsea fechas de FT que vienen duplicadas:
        'Wednesday, February 11, 2026Wed, Feb 11, 2026' -> '2026-02-11'
        """
        if not date_str or not date_str.strip():
            return None
        
        # Buscar patron corto (Mon, Tue, Wed, etc)
        date_parts = None
        for day_abbr in ['Mon, ', 'Tue, ', 'Wed, ', 'Thu, ', 'Fri, ', 'Sat, ', 'Sun, ']:
            if day_abbr in date_str:
                date_parts = date_str.split(day_abbr)
                break
        
        # Si se encontro separador, usar parte corta
        if date_parts and len(date_parts) > 1:
            date_clean = date_parts[1].strip()
        else:
            date_clean = date_str.strip()
        
        # Intentar parsear
        formats = ["%b %d, %Y", "%B %d, %Y", "%d %b %Y", "%d %B %Y"]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_clean, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        logger.debug(f"FT: No se pudo parsear fecha '{date_str}'")
        return None
    
    def scrape(self, isin):
        url = self.BASE_URL.format(isin=isin)
        logger.info(f"FT: {url}")
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Extraer nombre del fondo
            name_elem = soup.find('h1', class_='mod-tearsheet-overview__header__name')
            if not name_elem:
                name_elem = soup.find('h1')
            fund_name = name_elem.get_text(strip=True) if name_elem else ""
            
            # Buscar tabla de precios
            table = soup.find('table', class_='mod-tearsheet-historical-prices__results')
            
            if not table:
                logger.warning(f"FT: Tabla no encontrada para {isin}")
                return {"name": fund_name, "currency": "EUR", "prices": []}
            
            # Buscar tbody o usar filas directamente
            tbody = table.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
            else:
                all_rows = table.find_all('tr')
                rows = all_rows[1:] if len(all_rows) > 1 else []
            
            logger.info(f"FT: {len(rows)} filas encontradas")
            
            prices = []
            for idx, row in enumerate(rows):
                cols = row.find_all('td')
                
                # Validar que tenga suficientes columnas
                if len(cols) < 5:
                    logger.debug(f"FT: Fila {idx} tiene solo {len(cols)} columnas, saltando")
                    continue
                
                # Extraer fecha (columna 0) y precio close (columna 4)
                date_raw = cols[0].get_text(strip=True)
                close_price_raw = cols[4].get_text(strip=True)
                
                # Parsear fecha
                date_parsed = self.parse_date_ft(date_raw)
                if not date_parsed:
                    continue
                
                # Parsear precio
                try:
                    # Limpiar precio: eliminar comas, espacios
                    price_clean = close_price_raw.replace(',', '').replace(' ', '').strip()
                    
                    # Ignorar precios vacíos o cero
                    if not price_clean or price_clean in ['0', '0.00', '0.0', '00.00', '-']:
                        logger.debug(f"FT: Precio invalido '{close_price_raw}' en {date_parsed}")
                        continue
                    
                    price_value = float(price_clean)
                    
                    # Validar rango razonable (0.01 a 1,000,000)
                    if price_value < 0.01 or price_value > 1000000:
                        logger.warning(f"FT: Precio fuera de rango {price_value} en {date_parsed}")
                        continue
                    
                    prices.append({
                        "date": date_parsed,
                        "price": round(price_value, 4),  # Redondear a 4 decimales
                        "source": "ft",
                        "priority": 20
                    })
                    
                except (ValueError, IndexError) as e:
                    logger.debug(f"FT: Error parseando precio '{close_price_raw}': {e}")
                    continue
            
            logger.info(f"FT: {len(prices)} precios obtenidos para {isin}")
            
            return {"name": fund_name, "currency": "EUR", "prices": prices}
        
        except requests.exceptions.Timeout:
            logger.error(f"FT: Timeout al conectar para {isin}")
            return {"name": "", "currency": "EUR", "prices": []}
        except requests.exceptions.RequestException as e:
            logger.error(f"FT: Error de conexion para {isin}: {str(e)}")
            return {"name": "", "currency": "EUR", "prices": []}
        except Exception as e:
            logger.error(f"FT: Error inesperado para {isin}: {str(e)}")
            return {"name": "", "currency": "EUR", "prices": []}
