import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import sys
from pathlib import Path

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
    
    def scrape(self, id_instr):
        # Calcular rango de fechas: desde hace 20 años hasta hoy
        end_date = datetime.now()
        start_date = end_date - timedelta(days=20*365)
        
        # Formato de fechas para Fundsquare: DD/MM/YYYY
        date_debut = start_date.strftime("%d/%m/%Y")
        date_fin = end_date.strftime("%d/%m/%Y")
        
        # URL con parámetros de fecha
        url = f"{self.BASE_URL}?idInstr={id_instr}&dateDebut={date_debut}&dateFin={date_fin}"
        
        logger.info(f"Fundsquare: {url}")
        logger.info(f"Fundsquare: Solicitando datos desde {date_debut} hasta {date_fin}")
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            table = soup.find('table', class_='tabHorizontal')
            if not table:
                logger.warning(f"Fundsquare: Tabla no encontrada para {id_instr}")
                return {"prices": []}
            
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
            
            logger.info(f"Fundsquare: {len(prices)} precios obtenidos")
            return {"prices": prices}
        
        except Exception as e:
            logger.error(f"Fundsquare Error para {id_instr}: {str(e)}")
            return {"prices": []}
