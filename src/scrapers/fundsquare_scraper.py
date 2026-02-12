import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sys
from pathlib import Path

# Añadir directorio raíz al path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.logger import setup_logger

logger = setup_logger(__name__)

class FundsquareScraper:
    BASE_URL = "https://www.fundsquare.net/security/histo-prices?idInstr={id_instr}"
    
    def __init__(self, timeout=30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def parse_date_fs(self, date_str):
        """Convierte '11/02/2026' a '2026-02-11'"""
        try:
            dt = datetime.strptime(date_str, "%d/%m/%Y")
            return dt.strftime("%Y-%m-%d")
        except:
            return None
    
    def scrape(self, id_instr):
        """
        Devuelve:
        {
            "prices": [
                {"date": "2026-02-11", "price": 32.763, "source": "fundsquare", "priority": 10},
                ...
            ]
        }
        """
        url = self.BASE_URL.format(id_instr=id_instr)
        logger.info(f"🌐 Fundsquare: {url}")
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            table = soup.find('table', class_='tabHorizontal')
            if not table:
                logger.warning(f"⚠️  Fundsquare: Tabla no encontrada para {id_instr}")
                return {"prices": []}
            
            prices = []
            rows = table.find_all('tr')[2:]  # Saltar header y fila de iconos
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    date_raw = cols[0].get_text(strip=True)
                    price_raw = cols[1].get_text(strip=True)
                    
                    date_parsed = self.parse_date_fs(date_raw)
                    if not date_parsed:
                        continue
                    
                    try:
                        # "32.763 EUR " -> 32.763
                        # "1 234.56 EUR" -> 1234.56
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
                        logger.warning(f"No se pudo parsear precio: '{price_raw}'")
                        continue
            
            logger.info(f"✓ Fundsquare: {len(prices)} precios obtenidos")
            return {"prices": prices}
        
        except Exception as e:
            logger.error(f"❌ Fundsquare Error para {id_instr}: {e}")
            return {"prices": []}
